package service

import (
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ikennarichard/genderize-classifier/internal/client"
	"github.com/ikennarichard/genderize-classifier/internal/entity"
)


type APIError struct{ Message string }

func (e *APIError) Error() string { return e.Message }

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


func BuildProfile(name string) (*entity.Profile, *APIError) {
	res := fetchAll(name)

	if res.genderErr != nil || res.gender.Gender == nil || res.gender.Count == 0 {
		return nil, &APIError{"Genderize returned an invalid response"}
	}
	if res.ageErr != nil || res.age.Age == nil {
		return nil, &APIError{"Agify returned an invalid response"}
	}
	if res.natErr != nil || len(res.nationality.Country) == 0 {
		return nil, &APIError{"Nationalize returned an invalid response"}
	}

	country := topCountry(res.nationality.Country)
	id, _ := uuid.NewV7()

	return &entity.Profile{
		ID:                 id.String(),
		Name:               name,
		Gender:             *res.gender.Gender,
		GenderProbability:  res.gender.Probability,
		SampleSize:         res.gender.Count,
		Age:                *res.age.Age,
		AgeGroup:           ageGroup(*res.age.Age),
		CountryID:          country.CountryID,
		CountryProbability: country.Probability,
		CreatedAt:          time.Now().UTC().Format(time.RFC3339),
	}, nil
}