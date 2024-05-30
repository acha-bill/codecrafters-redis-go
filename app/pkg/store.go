package pkg

import (
	"fmt"
	"sync"
	"time"
)

type StoreVal struct {
	val       string
	ex        time.Time
	canExpire bool
}

func (s *StoreVal) String() string {
	return s.val
}

type Store struct {
	store map[string]*StoreVal
	mu    sync.RWMutex
}

func NewStore() *Store {
	return &Store{store: make(map[string]*StoreVal)}
}

func (s *Store) Get(k string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	v, ok := s.store[k]
	if !ok {
		return "", false
	}
	if v.canExpire && time.Now().After(v.ex) {
		return "", false
	}
	return v.val, true
}

func (s *Store) Set(k string, v string, px time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[k] = &StoreVal{
		val:       v,
		ex:        time.Now().Add(px),
		canExpire: px > 0,
	}
}

func (s *Store) Print() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return fmt.Sprintf("%+v", s.store)
}

func (s *Store) Load(path string) error {
	d, err := readDDB(path)
	if err != nil {
		return err
	}
	for k, v := range d {
		s.store[k] = &v
	}
	return nil
}
