package handler

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