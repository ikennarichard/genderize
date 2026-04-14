package handler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/ikennarichard/genderize-classifier/internal/entity"
	"github.com/ikennarichard/genderize-classifier/internal/service"
	"github.com/ikennarichard/genderize-classifier/internal/store"
)

type Handler struct{ store *store.Store }

func New(s *store.Store) *Handler { return &Handler{store: s} }

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/profiles", h.createProfile)
	mux.HandleFunc("GET /api/profiles/{id}", h.getProfile)
	mux.HandleFunc("GET /api/profiles", h.listProfiles)
	mux.HandleFunc("DELETE /api/profiles/{id}", h.deleteProfile)
}

func (h *Handler) createProfile(w http.ResponseWriter, r *http.Request) {
	var req entity.CreateProfileRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respond(w, http.StatusUnprocessableEntity, entity.ErrorResponse{
			Status: "error", Message: "invalid request body",
		})
		return
	}

	name, ok := req.Name.(string)
	if !ok {
		respond(w, http.StatusUnprocessableEntity, entity.ErrorResponse{
			Status: "error", Message: "name must be a string",
		})
		return
	}

	name = strings.TrimSpace(name)
	if name == "" {
		respond(w, http.StatusBadRequest, entity.ErrorResponse{
			Status: "error", Message: "name is required",
		})
		return
	}

	if existing, found := h.store.GetByName(name); found {
		respond(w, http.StatusOK, entity.ProfileResponse{
			Status:  "success",
			Message: "Profile already exists",
			Data:    existing,
		})
		return
	}

	profile, apiErr := service.BuildProfile(name)
	if apiErr != nil {
		respond(w, http.StatusBadGateway, entity.ErrorResponse{
			Status: "error", Message: apiErr.Message,
		})
		return
	}

	h.store.Save(profile)
	respond(w, http.StatusCreated, entity.ProfileResponse{Status: "success", Data: profile})
}

func (h *Handler) getProfile(w http.ResponseWriter, r *http.Request) {
	profile, ok := h.store.GetByID(r.PathValue("id"))
	if !ok {
		respond(w, http.StatusNotFound, entity.ErrorResponse{
			Status: "error", Message: "Profile not found",
		})
		return
	}
	respond(w, http.StatusOK, entity.SingleResponse{Status: "success", Data: profile})
}

func (h *Handler) listProfiles(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	summaries := h.store.List(q.Get("gender"), q.Get("country_id"), q.Get("age_group"))
	respond(w, http.StatusOK, entity.ListResponse{
		Status: "success",
		Count:  len(summaries),
		Data:   summaries,
	})
}

func (h *Handler) deleteProfile(w http.ResponseWriter, r *http.Request) {
	if !h.store.Delete(r.PathValue("id")) {
		respond(w, http.StatusNotFound, entity.ErrorResponse{
			Status: "error", Message: "Profile not found",
		})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func respond(w http.ResponseWriter, status int, payload any) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(payload); err != nil {
		http.Error(w, `{"status":"error","message":"internal server error"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	buf.WriteTo(w)
}