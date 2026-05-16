package service

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ikennarichard/insighta/internal/client"
	"github.com/ikennarichard/insighta/internal/domain"
)

const (
	maxFileSize  = 100 << 20 // 100MB
	chunkSize    = 500        // rows per batch insert
	maxWorkers   = 4          // concurrent chunk workers
)


type ImportResult struct {
	Status    string         `json:"status"`
	TotalRows int            `json:"total_rows"`
	Inserted  int            `json:"inserted"`
	Skipped   int            `json:"skipped"`
	Reasons   map[string]int `json:"reasons"`
}

func (s *ProfileService) ProcessCSV(ctx context.Context, r io.Reader) ImportResult {
	result := ImportResult{
		Reasons: make(map[string]int),
	}

	reader := csv.NewReader(r)
	reader.FieldsPerRecord = -1
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true

	header, err := reader.Read()
	if err != nil {
		result.Reasons["malformed_file"]++
		return result
	}

	colIndex, missingCols := parseHeader(header)
	if len(missingCols) > 0 {
		result.Reasons["missing_required_columns"]++
		return result
	}

	type chunk struct {
		profiles []domain.Profile
	}

	chunkCh := make(chan chunk, maxWorkers*2)
	var wg sync.WaitGroup
	var mu sync.Mutex

	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ch := range chunkCh {
				inserted, skipReasons := s.BulkInsert(ctx, ch.profiles)
				mu.Lock()
				result.Inserted += inserted
				for reason, count := range skipReasons {
					result.Reasons[reason] += count
					result.Skipped += count
				}
				mu.Unlock()
			}
		}()
	}

	batch := make([]domain.Profile, 0, chunkSize)

	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			mu.Lock()
			result.TotalRows++
			result.Skipped++
			result.Reasons["malformed_row"]++
			mu.Unlock()
			continue
		}

		mu.Lock()
		result.TotalRows++
		mu.Unlock()

		profile, reason := validateRow(row, colIndex)
		if reason != "" {
			mu.Lock()
			result.Skipped++
			result.Reasons[reason]++
			mu.Unlock()
			continue
		}

		batch = append(batch, *profile)

		if len(batch) >= chunkSize {
			toSend := make([]domain.Profile, len(batch))
			copy(toSend, batch)
			chunkCh <- chunk{profiles: toSend}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		chunkCh <- chunk{profiles: batch}
	}

	close(chunkCh)
	wg.Wait()

	return result
}

func (s *ProfileService) BulkInsert(ctx context.Context, profiles []domain.Profile) (int, map[string]int) {
	if len(profiles) == 0 {
		return 0, nil
	}

	err := s.repo.BulkCreateProfiles(ctx, profiles)
	if err == nil {
		return len(profiles), nil
	}

	slog.Warn("bulk insert failed, falling back to row-by-row", "error", err)

	inserted := 0
	reasons := make(map[string]int)

	for _, p := range profiles {
		if err := s.repo.CreateProfile(ctx, &p); err != nil {
			errStr := err.Error()
			switch {
			case strings.Contains(errStr, "unique") || strings.Contains(errStr, "duplicate"):
				reasons["duplicate_name"]++
			default:
				reasons["insert_error"]++
			}
		} else {
			inserted++
		}
	}

	return inserted, reasons
}

func parseHeader(header []string) (map[string]int, []string) {
	required := []string{"name"}
	optional := []string{"gender", "gender_probability", "age", "age_group",
		"country_id", "country_name", "country_probability", "sample_size"}

	index := make(map[string]int)
	for i, col := range header {
		index[strings.ToLower(strings.TrimSpace(col))] = i
	}

	var missing []string
	for _, col := range required {
		if _, ok := index[col]; !ok {
			missing = append(missing, col)
		}
	}
	_ = optional
	return index, missing
}

func validateRow(row []string, cols map[string]int) (*domain.Profile, string) {
	get := func(key string) string {
		i, ok := cols[key]
		if !ok || i >= len(row) {
			return ""
		}
		return strings.TrimSpace(row[i])
	}

	name := get("name")
	if name == "" {
		return nil, "missing_fields"
	}

	gender := strings.ToLower(get("gender"))
	if gender != "" && gender != "male" && gender != "female" {
		return nil, "invalid_gender"
	}

	var age int
	if ageStr := get("age"); ageStr != "" {
		a, err := strconv.Atoi(ageStr)
		if err != nil || a < 0 || a > 150 {
			return nil, "invalid_age"
		}
		age = a
	}

	parseProb := func(key string) (float64, bool) {
		s := get(key)
		if s == "" {
			return 0, true
		}
		v, err := strconv.ParseFloat(s, 64)
		if err != nil || v < 0 || v > 1 {
			return 0, false
		}
		return v, true
	}

	genderProb, ok := parseProb("gender_probability")
	if !ok {
		return nil, "invalid_gender_probability"
	}
	countryProb, ok := parseProb("country_probability")
	if !ok {
		return nil, "invalid_country_probability"
	}

	sampleSize := 0
	if s := get("sample_size"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 0 {
			return nil, "invalid_sample_size"
		}
		sampleSize = n
	}

	id, _ := uuid.NewV7()
	return &domain.Profile{
		ID:                 id,
		Name:               name,
		Gender:             gender,
		GenderProbability:  genderProb,
		Age:                age,
		AgeGroup:           get("age_group"),
		CountryID:          strings.ToUpper(get("country_id")),
		CountryName:        get("country_name"),
		CountryProbability: countryProb,
		SampleSize:         sampleSize,
		CreatedAt:          time.Now().UTC(),
	}, ""
}

func buildFilterQuery(f *domain.ProfileFilters) (string, []any) {
	query := `FROM profiles WHERE 1=1`
	var args []any
	argID := 1

	addCondition := func(condition string, value any) {
		query += fmt.Sprintf(" AND "+condition, argID)
		args = append(args, value)
		argID++
	}

	if f.Gender != "" {
		addCondition("gender ILIKE $%d", f.Gender)
	}

	if f.AgeGroup != "" {
		addCondition("age_group ILIKE $%d", f.AgeGroup)
	}

	if f.CountryID != "" {
		query += fmt.Sprintf(" AND (country_id ILIKE $%d OR country_name ILIKE $%d)", argID, argID+1)
		args = append(args, f.CountryID, f.CountryID+"%")
		argID += 2
	}

	if f.MinAge != nil {
		addCondition("age >= $%d", *f.MinAge)
	}
	if f.MaxAge != nil {
		addCondition("age <= $%d", *f.MaxAge)
	}

	if f.MinGenderProb != nil {
		addCondition("gender_probability >= $%d", *f.MinGenderProb)
	}
	if f.MinCountryProb != nil {
		addCondition("country_probability >= $%d", *f.MinCountryProb)
	}

	return query, args
}

func BuildProfile(name string) (*domain.Profile, error) {
	res := fetchAll(name)

	if res.genderErr != nil || res.gender.Gender == nil || res.gender.Count == 0 {
		return nil, newApiError("Genderize")
	}
	if res.ageErr != nil || res.age.Age == nil {
		return nil, newApiError("Agify")
	}
	if res.natErr != nil || len(res.nationality.Country) == 0 {
		return nil, newApiError("Nationalize")
	}

	country := topCountry(res.nationality.Country)
	id, _ := uuid.NewV7()

	return &domain.Profile{
		ID:                 id,
		Name:               name,
		Gender:             *res.gender.Gender,
		GenderProbability:  res.gender.Probability,
		SampleSize:         res.gender.Count,
		Age:                *res.age.Age,
		AgeGroup:           ageGroup(*res.age.Age),
		CountryID:          country.CountryID,
		CountryName:        resolveCountryName(country.CountryID),
		CountryProbability: country.Probability,
		CreatedAt:          time.Now().UTC(),
	}, nil
}


func newApiError(apiName string) error {
    return fmt.Errorf("%s returned an invalid response", apiName)
}

func ageGroup(age int) string {
	switch {
	case age <= 12:
		return "child"
	case age <= 19:
		return "teenager"
	case age <= 59:
		return "adult"
	default:
		return "senior"
	}
}

func topCountry(countries []client.Country) client.Country {
	top := countries[0]
	for _, c := range countries[1:] {
		if c.Probability > top.Probability {
			top = c
		}
	}
	return top
}

type results struct {
	gender      *client.GenderizeResponse
	age         *client.AgifyResponse
	nationality *client.NationalizeResponse
	genderErr   error
	ageErr      error
	natErr      error
}

func fetchAll(name string) results {
	var (
		wg  sync.WaitGroup
		res results
	)
	wg.Add(3)

	go func() { defer wg.Done(); res.gender, res.genderErr = client.FetchGenderize(name) }()
	go func() { defer wg.Done(); res.age, res.ageErr = client.FetchAgify(name) }()
	go func() { defer wg.Done(); res.nationality, res.natErr = client.FetchNationalize(name) }()

	wg.Wait()
	return res
}


func resolveCountryName(code string) string {
	countries := map[string]string{
		"AF": "Afghanistan",
		"AL": "Albania",
		"DZ": "Algeria",
		"AR": "Argentina",
		"AU": "Australia",
		"AT": "Austria",
		"BD": "Bangladesh",
		"BE": "Belgium",
		"BR": "Brazil",
		"BG": "Bulgaria",
		"CA": "Canada",
		"CL": "Chile",
		"CN": "China",
		"CO": "Colombia",
		"HR": "Croatia",
		"CZ": "Czech Republic",
		"DK": "Denmark",
		"EG": "Egypt",
		"ET": "Ethiopia",
		"FI": "Finland",
		"FR": "France",
		"DE": "Germany",
		"GH": "Ghana",
		"GR": "Greece",
		"HU": "Hungary",
		"IN": "India",
		"ID": "Indonesia",
		"IQ": "Iraq",
		"IE": "Ireland",
		"IL": "Israel",
		"IT": "Italy",
		"JP": "Japan",
		"KE": "Kenya",
		"MY": "Malaysia",
		"MX": "Mexico",
		"MA": "Morocco",
		"NL": "Netherlands",
		"NZ": "New Zealand",
		"NG": "Nigeria",
		"NO": "Norway",
		"PK": "Pakistan",
		"PE": "Peru",
		"PH": "Philippines",
		"PL": "Poland",
		"PT": "Portugal",
		"RO": "Romania",
		"RU": "Russia",
		"SA": "Saudi Arabia",
		"ZA": "South Africa",
		"ES": "Spain",
		"SE": "Sweden",
		"CH": "Switzerland",
		"TZ": "Tanzania",
		"TH": "Thailand",
		"TN": "Tunisia",
		"TR": "Turkey",
		"UG": "Uganda",
		"UA": "Ukraine",
		"AE": "United Arab Emirates",
		"GB": "United Kingdom",
		"US": "United States",
		"VN": "Vietnam",
		"ZW": "Zimbabwe",
	}

	if name, ok := countries[code]; ok {
		return name
	}
	return code
}