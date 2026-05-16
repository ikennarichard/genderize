package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ikennarichard/insighta/internal/cache"
	"github.com/ikennarichard/insighta/internal/service"
	"github.com/ikennarichard/insighta/internal/utils"
)

type ProfileHandler struct {
	srv *service.ProfileService 
  cache *cache.Cache
}

const (
	maxFileSize  = 100 << 20 // 100MB
	chunkSize    = 500        // rows per batch insert
	maxWorkers   = 4          // concurrent chunk workers
)



func NewProfileHandler(srv *service.ProfileService, cache *cache.Cache) *ProfileHandler {
	return &ProfileHandler{srv: srv, cache: cache}
}

func (h *ProfileHandler) DeleteProfile(w http.ResponseWriter, r *http.Request) {
    id := r.PathValue("id")
    if err := h.srv.DeleteProfile(r.Context(), id); err != nil {
        utils.RespondError(w, http.StatusNotFound, "Profile not found")
        return
    }
    if h.cache != nil {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			h.cache.InvalidateProfileID(ctx, id)
			h.cache.InvalidateListCache(ctx)
		}()
	}
    w.WriteHeader(http.StatusNoContent)
}


func (h *ProfileHandler) SearchProfiles(w http.ResponseWriter, r *http.Request) {
    queryStr := strings.TrimSpace(r.URL.Query().Get("q"))
    if queryStr == "" {
        utils.RespondError(w, http.StatusBadRequest, "Query parameter 'q' is required")
        return
    }

    filters, err := service.ParseNaturalLanguage(queryStr)
		
    if err != nil {
        utils.RespondError(w, http.StatusBadRequest, "Unable to interpret query")
        return
    }



    page, _ := strconv.Atoi(r.URL.Query().Get("page"))
    limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
    if page < 1 {
        page = 1
    }
    if limit <= 0 || limit > 50 {
        limit = 10
    }

    if err := filters.Validate(); err != nil {
        utils.RespondError(w, http.StatusBadRequest, "Invalid query parameters")
        return
    }

				// Normalize parsed filters — "women aged 20-45 in Nigeria" and
// "Nigerian females between 20 and 45" now produce the same cache key
normalizedFilters := service.NormalizeFilters(filters)
key := service.NormalizedCacheKey(normalizedFilters, page, limit)

	// Check cache first
	if h.cache != nil {
		var cached utils.PaginatedResponse
		if hit, _ := h.cache.Get(r.Context(), key, &cached); hit {
			utils.Respond(w, http.StatusOK, cached)
			return
		}
	}

    profiles, total, err := h.srv.GetFilteredProfiles(r.Context(), normalizedFilters, page, limit)
    if err != nil {
        utils.RespondError(w, http.StatusInternalServerError, "Database error")
        return
    }

    data := mapToDTOs(profiles)

    resp := utils.BuildPaginatedResponse(
        data,
        page,
        limit,
        total,
        "/api/profiles/search",
        r.URL.Query(),
    )

    utils.Respond(w, http.StatusOK, resp)
}

func (h *ProfileHandler) CreateProfile(w http.ResponseWriter, r *http.Request) {
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

	existing, err := h.srv.GetProfileByName(r.Context(), name)
	if err == nil && existing != nil {
        dataResponse := fromDomain(existing)
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

	if err := h.srv.CreateProfile(r.Context(), profile); err != nil {
		slog.Error("failed to create profile", 
            "error", err, 
            "user_id", r.Context().Value("user_id"),
        )
		utils.RespondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	createdRes := fromDomain(profile)
    	if h.cache != nil {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			h.cache.InvalidateListCache(ctx)
		}()
	}
	utils.Respond(w, http.StatusCreated, ProfileResponse{Status: "success", Data: &createdRes})
	return
}

func (h *ProfileHandler) GetProfile(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
    	if h.cache != nil {
		var cached ProfileResponse
		if hit, _ := h.cache.Get(r.Context(), cache.IDKey(id), &cached); hit {
			utils.Respond(w, http.StatusOK, cached)
			return
		}
	}
	profile, err := h.srv.GetProfileByID(r.Context(), id)
	if err != nil {
		fmt.Println("GetProfile Error:", err)
		utils.RespondError(w, http.StatusNotFound, "Profile not found")
		return
	}
	profileRes := fromDomain(profile)
   resp := ProfileResponse{Status: "success", Data: &profileRes}

	if h.cache != nil {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			h.cache.Set(ctx, cache.IDKey(id), resp, cache.ProfileByIDTTL)
		}()
	}

	utils.Respond(w, http.StatusOK, resp)
}

func (h *ProfileHandler) ListProfiles(w http.ResponseWriter, r *http.Request) {
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

		page, err := strconv.Atoi(q.Get("page"))
		if err != nil || page < 1 {
        page = 1
    }
    limit, err := strconv.Atoi(q.Get("limit"))
		if err != nil || limit <= 0 {
        limit = 10
    }
    if limit > 50 {
        limit = 50
    }

    filters := h.parseFilters(r)

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

    if err := filters.Validate(); err != nil {
        utils.RespondError(w, http.StatusBadRequest, "Invalid query parameters")
        return
    }

    	// Build cache key from all query params
	cacheParams := map[string]string{
		"gender":     filters.Gender,
		"country_id": filters.CountryID,
		"age_group":  filters.AgeGroup,
		"sort_by":    filters.SortBy,
		"order":      filters.Order,
		"page":       fmt.Sprintf("%d", page),
		"limit":      fmt.Sprintf("%d", limit),
	}
	if filters.MinAge != nil {
		cacheParams["min_age"] = fmt.Sprintf("%d", *filters.MinAge)
	}
	if filters.MaxAge != nil {
		cacheParams["max_age"] = fmt.Sprintf("%d", *filters.MaxAge)
	}

normalizedFilters := service.NormalizeFilters(filters)
key := service.NormalizedCacheKey(normalizedFilters, page, limit)

	// Check cache first
	if h.cache != nil {
		var cached utils.PaginatedResponse
		if hit, _ := h.cache.Get(r.Context(), key, &cached); hit {
			utils.Respond(w, http.StatusOK, cached)
			return
		}
	}

		profiles, total, err := h.srv.ListProfiles(r.Context(), normalizedFilters, page, limit)
    if err != nil {
			fmt.Println("GetFiltered Error:", err.Error())
        utils.RespondError(w, 500, "Database failure")
        return
    }

    data := mapToDTOs(profiles)


     resp := utils.BuildPaginatedResponse(
        data, 
        page, 
        limit, 
        total, 
        "/api/v1/profiles", 
        r.URL.Query(),
    )

	if h.cache != nil {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			h.cache.Set(ctx, key, resp, cache.ProfileListTTL)
		}()
	}
		utils.Respond(w, http.StatusOK, resp)
}

func (h *ProfileHandler) ImportProfiles(w http.ResponseWriter, r *http.Request) {
	// Limit memory used for multipart parsing — stream the rest to disk
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		utils.RespondError(w, http.StatusBadRequest, "Failed to parse form")
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		utils.RespondError(w, http.StatusBadRequest, "Missing file field")
		return
	}
	defer file.Close()

	if header.Size > maxFileSize {
		utils.RespondError(w, http.StatusRequestEntityTooLarge,
			fmt.Sprintf("File exceeds maximum size of %dMB", maxFileSize>>20))
		return
	}

	slog.Info("csv import started",
		"filename", header.Filename,
		"size_bytes", header.Size,
	)

	result := h.srv.ProcessCSV(r.Context(), file)
	result.Status = "success"

	// Invalidate list cache after bulk insert
	if h.cache != nil && result.Inserted > 0 {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			h.cache.InvalidateListCache(ctx)
		}()
	}

	slog.Info("csv import complete",
		"total", result.TotalRows,
		"inserted", result.Inserted,
		"skipped", result.Skipped,
	)

	utils.Respond(w, http.StatusOK, result)
}
