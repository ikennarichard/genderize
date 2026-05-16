package service

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/ikennarichard/insighta/internal/domain"
	"github.com/ikennarichard/insighta/internal/repository"
)

type ProfileService struct {
	repo *repository.PostgresProfileRepository
}

func NewProfileService (repo *repository.PostgresProfileRepository) *ProfileService {
	return &ProfileService{
		repo: repo,
	}
}

func (h *ProfileService) CreateProfile(ctx context.Context, profile *domain.Profile) error {
	if err := h.repo.CreateProfile(ctx, profile); err != nil {
		slog.Error("failed to create profile", 
            "error", err, 
            "user_id", ctx.Value("user_id"),
        )

		return err
	}
	return nil
}

func (s *ProfileService) UpdateProfile(ctx context.Context, p *domain.Profile) error {
	
	err := s.repo.UpdateProfile(ctx, p)
	if err != nil {
		return fmt.Errorf("failed to update profile: %w", err)
	}

	return nil
}

func (s *ProfileService) DeleteProfile(ctx context.Context, id string) error {
    if err := s.repo.DeleteProfile(ctx, id); err != nil {
        return err
    }
		return nil
}

func (s *ProfileService) ListProfiles(ctx context.Context, filters *domain.ProfileFilters, page int, limit int) ([]domain.Profile, int, error) {

		profiles, total, err := s.GetFilteredProfiles(ctx, filters, page, limit)
    if err != nil {
			fmt.Println("GetFiltered Error:", err.Error())
        return nil, 0, err
    }
		return profiles, total, err
}

func (s *ProfileService) GetProfileByName(ctx context.Context, name string) (*domain.Profile, error) {
	profile, err := s.repo.GetProfileByName(ctx, name)
	if err == nil && profile != nil {

        return profile, nil
    }

		return nil, err
}

func (s *ProfileService) GetFilteredProfiles(
	ctx context.Context,
	f *domain.ProfileFilters,
	page int,
	limit int,
) ([]domain.Profile, int, error) {

	// 1. Build basic filters
	baseQuery, args := buildFilterQuery(f)

	// 2. Validate & Sanitize Sorting Input
	allowedSortColumns := map[string]string{
		"age":                "age",
		"name":               "name",
		"created_at":         "created_at",
		"gender_probability": "gender_probability",
	}

	sortBy := "created_at"
	if col, ok := allowedSortColumns[f.SortBy]; ok {
		sortBy = col
	}

	order := "DESC"
	if strings.ToLower(f.Order) == "asc" {
		order = "ASC"
	}

	// 3. Apply Pagination Sanitization rules
	if limit <= 0 || limit > 50 {
		limit = 10
	}
	if page < 1 {
		page = 1
	}
	offset := (page - 1) * limit

	// 4. Delegate DB execution and hydration completely to the repo layer
	return s.repo.FindFiltered(ctx, baseQuery, args, sortBy, order, limit, offset)
}

func (s *ProfileService) GetAllFilteredProfiles(
	ctx context.Context,
	f *domain.ProfileFilters,
) ([]domain.Profile, error) {


	baseQuery, args := buildFilterQuery(f)

	allowedSortColumns := map[string]string{
		"age":                "age",
		"name":               "name",
		"created_at":         "created_at",
		"gender_probability": "gender_probability",
	}

	sortBy := "created_at"
	if col, ok := allowedSortColumns[f.SortBy]; ok {
		sortBy = col
	}

	order := "DESC"
	if strings.ToLower(f.Order) == "asc" {
		order = "ASC"
	}

	return s.repo.FindAllFiltered(ctx, baseQuery, args, sortBy, order)
}

func (s *ProfileService) GetProfileByID(ctx context.Context, id string) (*domain.Profile, error) {
	profile, err := s.repo.GetProfileByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to get profile by id: %w", err)
	}
	
	return profile, nil
}