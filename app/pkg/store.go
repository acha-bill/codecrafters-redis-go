package pkg

import "sync"

type Store struct {
	store map[string]string
	mu    sync.RWMutex
}

func NewStore() *Store {
	return &Store{store: make(map[string]string)}
}

func (s *Store) Get(k string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	v, ok := s.store[k]
	return v, ok
}

func (s *Store) Set(k, v string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[k] = v
}
