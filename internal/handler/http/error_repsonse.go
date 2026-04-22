package handler

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/ikennarichard/genderize-classifier/internal/domain"
)

func WriteError(w http.ResponseWriter, err error) {
    status := http.StatusInternalServerError
    message := "internal server error"

    switch {
    case errors.Is(err, domain.ErrNotFound):
        status = http.StatusNotFound
        message = err.Error()

    case errors.Is(err, domain.ErrAlreadyExists):
        status = http.StatusConflict
        message = err.Error()

    case errors.Is(err, domain.ErrUnauthorized):
        status = http.StatusUnauthorized
        message = err.Error()

    case errors.Is(err, domain.ErrMissingName):
        status = http.StatusBadRequest
        message = err.Error()

    case errors.Is(err, domain.ErrInvalidResponse):
        status = http.StatusBadRequest
        message = err.Error()
    }

    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(status)

    _ = json.NewEncoder(w).Encode(domain.ErrorResponse{
        Status:  "error",
        Message: message,
    })
}