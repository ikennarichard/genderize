package store

import (
	"strings"
	"sync"

	"github.com/ikennarichard/genderize-classifier/internal/entity"
)

type Store struct {
	mu     sync.RWMutex
	byID   map[string]*entity.Profile
	byName map[string]*entity.Profile
}

func New() *Store {
	return &Store{
		byID:   make(map[string]*entity.Profile),
		byName: make(map[string]*entity.Profile),
	}
}

func (s *Store) Save(p *entity.Profile) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byID[p.ID] = p
	s.byName[strings.ToLower(p.Name)] = p
}

func (s *Store) GetByID(id string) (*entity.Profile, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.byID[id]
	return p, ok
}

func (s *Store) GetByName(name string) (*entity.Profile, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.byName[strings.ToLower(name)]
	return p, ok
}

func (s *Store) Delete(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.byID[id]
	if !ok {
		return false
	}
	delete(s.byID, id)
	delete(s.byName, strings.ToLower(p.Name))
	return true
}

func (s *Store) List(gender, countryID, ageGroup string) []entity.ProfileSummary {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]entity.ProfileSummary, 0, len(s.byID))
	for _, p := range s.byID {
		if gender != "" && !strings.EqualFold(p.Gender, gender) {
			continue
		}
		if countryID != "" && !strings.EqualFold(p.CountryID, countryID) {
			continue
		}
		if ageGroup != "" && !strings.EqualFold(p.AgeGroup, ageGroup) {
			continue
		}
		out = append(out, entity.ProfileSummary{
			ID:        p.ID,
			Name:      p.Name,
			Gender:    p.Gender,
			Age:       p.Age,
			AgeGroup:  p.AgeGroup,
			CountryID: p.CountryID,
		})
	}
	return out
}