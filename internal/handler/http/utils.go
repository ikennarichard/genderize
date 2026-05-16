package handler

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ikennarichard/insighta/internal/domain"
)

type ProfileDTO struct {
	ID                 string  `json:"id"`
	Name               string  `json:"name"`
	Gender             string  `json:"gender"`
	GenderProbability  float64 `json:"gender_probability"`
	SampleSize         int     `json:"sample_size,omitempty"`
	Age                int     `json:"age"`
	AgeGroup           string  `json:"age_group"`
	CountryID          string  `json:"country_id"`
	CountryName        string  `json:"country_name,omitempty"`
	CountryProbability float64 `json:"country_probability"`
	CreatedAt          string  `json:"created_at"`
}

type ProfileSummary struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Gender    string `json:"gender"`
	Age       int    `json:"age"`
	AgeGroup  string `json:"age_group"`
	CountryID string `json:"country_id"`
}

type CreateProfileRequest struct {
	Name string `json:"name"`
}

type ErrorResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

type ProfileResponse struct {
	Status  string          `json:"status"`
	Message string          `json:"message,omitempty"`
	Data    *ProfileDTO `json:"data"`
}

func fromDomain(p *domain.Profile) ProfileDTO {
	return ProfileDTO{
		ID:                 p.ID.String(),
		Name:               p.Name,
		Gender:             p.Gender,
		GenderProbability:  p.GenderProbability,
		SampleSize:         p.SampleSize,
		Age:                p.Age,
		AgeGroup:           p.AgeGroup,
		CountryID:          p.CountryID,
		CountryName:        p.CountryName,
		CountryProbability: p.CountryProbability,
		CreatedAt:           p.CreatedAt.Format(time.RFC3339),
	}
}

func mapToDTOs(profiles []domain.Profile) []ProfileDTO {
	dtos := make([]ProfileDTO, len(profiles))
	
	for i, p := range profiles {
		dtos[i] = fromDomain(&p)
	}
	
	return dtos
}

func (h *ProfileHandler) parseFilters(r *http.Request) *domain.ProfileFilters {
	q := r.URL.Query()

	filters := domain.ProfileFilters{
		Gender:    strings.TrimSpace(q.Get("gender")),
		CountryID: strings.TrimSpace(q.Get("country_id")),
		AgeGroup:  strings.TrimSpace(q.Get("age_group")),
		SortBy:    strings.TrimSpace(strings.ToLower(q.Get("sort_by"))),
		Order:     strings.TrimSpace(strings.ToLower(q.Get("order"))),
	}

	if minAge, err := strconv.Atoi(q.Get("min_age")); err == nil {
		filters.MinAge = &minAge
	}
	if maxAge, err := strconv.Atoi(q.Get("max_age")); err == nil {
		filters.MaxAge = &maxAge
	}

	if minProb, err := strconv.ParseFloat(q.Get("min_gender_probability"), 64); err == nil {
		filters.MinGenderProb = &minProb
	}

	page, err := strconv.Atoi(q.Get("page"))
	if err != nil || page < 1 {
		page = 1
	}


	limit, err := strconv.Atoi(q.Get("limit"))
	if err != nil || limit < 1 {
		limit = 10
	} else if limit > 100 {
		limit = 100
	}

	return &filters
}
