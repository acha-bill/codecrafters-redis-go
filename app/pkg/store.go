package pkg

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

type StoreTypedValue struct {
	Type string
	Val  any
}

var (
	ErrSmallXaddID = fmt.Errorf("the ID specified in XADD is equal or smaller than the target stream top item")
	ErrZeroXaddID  = fmt.Errorf("the ID specified in XADD must be greater than 0-0")
)

type StreamEntry struct {
	ID     string
	Values map[string]string
}

type Stream struct {
	Entries []StreamEntry
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

func (s *Store) SetStream(k string, id string, px time.Duration) (string, error) {
	idParts := strings.Split(id, "-")
	if len(idParts) == 2 {
		if idParts[0] != "*" && idParts[1] == "*" {
			ms, _ := strconv.ParseInt(idParts[0], 10, 64)
			seq := s.generateSeq(k, ms)
			id = fmt.Sprintf("%d-%d", ms, seq)
		} else {
			err := s.validateStreamID(k, id)
			if err != nil {
				return "", err
			}
		}
	}

	if id == "*" {
		ms := time.Now().UnixMilli()
		seq := s.generateSeq(k, ms)
		id = fmt.Sprintf("%d-%d", ms, seq)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	storeVal, ok := s.store[k]
	if !ok {
		v := &StoreTypedValue{Type: "stream",
			Val: &Stream{
				Entries: []StreamEntry{
					{
						ID:     id,
						Values: make(map[string]string),
					},
				},
			},
		}
		s.store[k] = &StoreVal{
			val:       v,
			ex:        time.Now().Add(px),
			canExpire: px > 0,
		}
		return id, nil
	}

	stream := storeVal.val.Val.(*Stream)
	stream.Entries = append(stream.Entries, StreamEntry{
		ID:     id,
		Values: make(map[string]string),
	})
	return id, nil
}

func (s *Store) parseStreamId(id string) (int64, int, error) {
	parts := strings.Split(id, "-")
	var ms int64
	var seq int
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid stream id")
	}
	ms, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	seq, err = strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, err
	}
	return ms, seq, nil
}

func (s *Store) generateSeq(k string, ms int64) int {
	sv, _ := s.Get(k)
	if sv == nil {
		if ms == 0 {
			return 1
		}
		return 0
	}
	var lastSeq int
	empty := true
	for _, e := range sv.Val.(*Stream).Entries {
		entryMs, _, _ := s.parseStreamId(e.ID)
		if entryMs == ms {
			empty = false
			_, lastSeq, _ = s.parseStreamId(e.ID)
		}
	}

	fmt.Println("ms", ms, "last sequence", lastSeq, "empty", empty)
	if ms == 0 {
		return lastSeq + 1
	}

	if lastSeq == 0 && empty {
		return 0
	}
	return lastSeq + 1
}

func (s *Store) validateStreamID(k string, id string) error {
	ms, seq, err := s.parseStreamId(id)
	if err != nil {
		return err
	}

	var lastMs int64
	var lastSeq int
	sv, _ := s.Get(k)
	if sv != nil {
		entries := sv.Val.(*Stream).Entries
		last := entries[len(entries)-1]
		lastMs, lastSeq, err = s.parseStreamId(last.ID)
	}

	if ms == 0 && seq == 0 {
		return ErrZeroXaddID
	}

	if ms < lastMs {
		return ErrSmallXaddID
	}
	if seq <= lastSeq {
		return ErrSmallXaddID
	}
	return nil
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
