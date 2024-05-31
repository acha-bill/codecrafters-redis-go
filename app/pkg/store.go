package pkg

import (
	"fmt"
	"sync"
	"time"
)

type StoreTypedValue struct {
	Type string
	Val  string
}

type StoreVal struct {
	val       *StoreTypedValue
	ex        time.Time
	canExpire bool
}

type Store struct {
	store map[string]*StoreVal
	mu    sync.RWMutex
}

func NewStore() *Store {
	return &Store{store: make(map[string]*StoreVal)}
}

func (s *Store) Get(k string) (*StoreTypedValue, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	v, ok := s.store[k]
	if !ok {
		return nil, false
	}
	if v.canExpire && time.Now().After(v.ex) {
		return nil, false
	}
	return v.val, true
}

func (s *Store) SetString(k string, v string, px time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[k] = &StoreVal{
		val:       &StoreTypedValue{Type: "string", Val: v},
		ex:        time.Now().Add(px),
		canExpire: px > 0,
	}
}

func (s *Store) SetStream(k string, v string, px time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[k] = &StoreVal{
		val:       &StoreTypedValue{Type: "stream", Val: v},
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
