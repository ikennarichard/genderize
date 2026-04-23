package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/ikennarichard/genderize-classifier/internal/domain"
	"github.com/ikennarichard/genderize-classifier/internal/service"
	"github.com/ikennarichard/genderize-classifier/internal/utils"
)

type Handler struct {
	repo domain.ProfileRepository 
}

func New(repo domain.ProfileRepository) *Handler {
	return &Handler{repo: repo}
}

func (h *Handler) RegisterProfileRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/profiles", h.createProfile)
	mux.HandleFunc("GET /api/profiles/{id}", h.getProfile)
	mux.HandleFunc("GET /api/profiles", h.listProfiles)
	mux.HandleFunc("DELETE /api/profiles/{id}", h.deleteProfile)
}

func (h *Handler) createProfile(w http.ResponseWriter, r *http.Request) {
	var req CreateProfileRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		utils.RespondError(w, http.StatusUnprocessableEntity, "invalid request body")
		return
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		utils.RespondError(w, http.StatusBadRequest, "name is required")
		return
	}

	existing, err := h.repo.GetByName(r.Context(), name)
	if err == nil && existing != nil {
        dataResponse := fromDomain(existing) // Now this is safe
        utils.Respond(w, http.StatusOK, ProfileResponse{
            Status:  "success",
            Message: "Profile already exists",
            Data:    &dataResponse,
        })
        return
    }

	profile, err := service.BuildProfile(name)
	if err != nil {
		utils.RespondError(w, http.StatusBadGateway, err.Error())
		return
	}

	if err := h.repo.Create(r.Context(), profile); err != nil {
		utils.RespondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	createdRes := fromDomain(profile)
	utils.Respond(w, http.StatusCreated, ProfileResponse{Status: "success", Data: &createdRes})
}

func (h *Handler) getProfile(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	profile, err := h.repo.GetByID(r.Context(), id)
	// fmt.Println("profile by")
	if err != nil {
		utils.RespondError(w, http.StatusNotFound, "Profile not found")
		return
	}
	profileRes := fromDomain(profile)
	utils.Respond(w, http.StatusOK, ProfileResponse{Status: "success", Data: &profileRes})
}

func (h *Handler) listProfiles(w http.ResponseWriter, r *http.Request) {
    q := r.URL.Query()

    parseInt := func(key string) (*int, error) {
        val := q.Get(key)
        if val == "" { return nil, nil }
        i, err := strconv.Atoi(val)
        if err != nil { return nil, err }
        return &i, nil
    }

    parseFloat := func(key string) (*float64, error) {
        val := q.Get(key)
        if val == "" { return nil, nil }
        f, err := strconv.ParseFloat(val, 64)
        if err != nil { return nil, err }
        return &f, nil
    }

    filters := domain.ProfileFilters{
        Gender:    strings.TrimSpace(q.Get("gender")),
        AgeGroup:  strings.TrimSpace(q.Get("age_group")),
        CountryID: strings.TrimSpace(q.Get("country_id")),
    }

    var err error
    if filters.MinAge, err = parseInt("min_age"); err != nil {
        utils.RespondError(w, http.StatusUnprocessableEntity, "Invalid min_age parameter")
        return
    }
    if filters.MaxAge, err = parseInt("max_age"); err != nil {
        utils.RespondError(w, http.StatusUnprocessableEntity, "Invalid max_age parameter")
        return
    }
    if filters.MinGenderProb, err = parseFloat("min_gender_probability"); err != nil {
        utils.RespondError(w, http.StatusUnprocessableEntity, "Invalid min_gender_probability parameter")
        return
    }
    if filters.MinCountryProb, err = parseFloat("min_country_probability"); err != nil {
        utils.RespondError(w, http.StatusUnprocessableEntity, "Invalid min_country_probability parameter")
        return
    }

    profiles, err := h.repo.GetFiltered(r.Context(), filters)
    if err != nil {
        fmt.Println("list_profiles_failed", "error", err)
        utils.RespondError(w, http.StatusInternalServerError, "Internal server error")
        return
    }

    data := make([]ProfileDTO, len(profiles))
    for i, p := range profiles {
        data[i] = fromDomain(&p)
    }

    utils.Respond(w, http.StatusOK, map[string]any{
        "status": "success",
        "count":  len(data),
        "data":   data,
    })
}

func (h *Handler) deleteProfile(w http.ResponseWriter, r *http.Request) {
	err := h.repo.Delete(r.Context(), r.PathValue("id"))
	if err != nil {
		utils.RespondError(w, http.StatusNotFound, "Profile not found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
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
		CreatedAt:           p.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
	}
}