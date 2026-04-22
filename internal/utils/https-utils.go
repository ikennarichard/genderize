package utils

import (
	"encoding/json"
	"net/http"

	"github.com/ikennarichard/genderize-classifier/internal/domain"
)

func Respond(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func RespondError(w http.ResponseWriter, status int, message string) {
	Respond(w, status, domain.ErrorResponse{
		Status:  "error",
		Message: message,
	})
}