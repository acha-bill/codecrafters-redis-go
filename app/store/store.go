package store

import (
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/pkg"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
	"strconv"
	"strings"
	"sync"
	"time"
)

type TypedValue struct {
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

type Val struct {
	val       *TypedValue
	ex        time.Time
	canExpire bool
}

type Store struct {
	store map[string]*Val
	mu    sync.RWMutex

	lastStreamId map[string]string
}

func New() *Store {
	return &Store{
		store:        make(map[string]*Val),
		lastStreamId: make(map[string]string),
	}
}

func (s *Store) Get(k string) (*TypedValue, bool) {
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

	s.store[k] = &Val{
		val:       &TypedValue{Type: "string", Val: v},
		ex:        time.Now().Add(px),
		canExpire: px > 0,
	}
}

func (s *Store) SetStream(k string, id string, data map[string]string, px time.Duration) (string, error) {
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
		v := &TypedValue{Type: "stream",
			Val: &Stream{
				Entries: []StreamEntry{
					{
						ID:     id,
						Values: data,
					},
				},
			},
		}
		s.store[k] = &Val{
			val:       v,
			ex:        time.Now().Add(px),
			canExpire: px > 0,
		}
		s.lastStreamId[k] = id
		return id, nil
	}

	stream := storeVal.val.Val.(*Stream)
	stream.Entries = append(stream.Entries, StreamEntry{
		ID:     id,
		Values: data,
	})
	s.lastStreamId[k] = id
	return id, nil
}

func (s *Store) RangeStream(k, startId, endId string) []StreamEntry {
	sv, _ := s.Get(k)
	if sv == nil {
		return nil
	}

	skipSequenceCheck := func(id string) bool {
		return strings.Index(id, "-") < 0
	}
	inRange := func(id, start, end string) bool {
		idMs, idSeq, _ := s.parseStreamId(id)
		startMs, startSeq, _ := s.parseStreamId(start)

		msPass := idMs >= startMs
		seqPass := skipSequenceCheck(start) || idSeq >= startSeq
		startPass := msPass && seqPass

		endMs, endSeq, _ := s.parseStreamId(end)
		msPass = idMs <= endMs
		seqPass = skipSequenceCheck(end) || idSeq <= endSeq
		endPass := msPass && seqPass
		return startPass && endPass
	}

	if sv.Type != "stream" {
		return nil
	}
	stream := sv.Val.(*Stream)
	var res []StreamEntry
	for _, entry := range stream.Entries {
		if inRange(entry.ID, startId, endId) {
			res = append(res, entry)
		}
	}
	return res
}

type ReadStreamRes struct {
	Stream  string
	Entries []StreamEntry
}

func (s *Store) readStreams(req [][]string) []*ReadStreamRes {
	skipSequenceCheck := func(id string) bool {
		return strings.Index(id, "-") < 0
	}
	inRange := func(id, start string) bool {
		idMs, idSeq, _ := s.parseStreamId(id)
		startMs, startSeq, _ := s.parseStreamId(start)

		if idMs > startMs {
			return true
		}
		if idMs < startMs {
			return false
		}
		return skipSequenceCheck(start) || idSeq > startSeq
	}

	var res []*ReadStreamRes
	for _, streamReq := range req {
		k, startId := streamReq[0], streamReq[1]
		sv, _ := s.Get(k)
		if sv == nil {
			res = append(res, nil)
			continue
		}
		if sv.Type != "stream" {
			return nil
		}
		stream := sv.Val.(*Stream)
		var data []StreamEntry
		for _, entry := range stream.Entries {
			if inRange(entry.ID, startId) {
				data = append(data, entry)
			}
		}
		res = append(res, &ReadStreamRes{
			Stream:  k,
			Entries: data,
		})
	}
	return res
}
func (s *Store) ReadStream(req [][]string, block time.Duration) []*ReadStreamRes {
	res := s.readStreams(req)
	if block < 0 {
		return res
	}

	fmt.Println(s.lastStreamId)
	lastStreamIds1 := utils.MapCopy(s.lastStreamId)
	fmt.Println(lastStreamIds1)
	updated := func() bool {
		u := true
		lastStreamIds2 := utils.MapCopy(s.lastStreamId)
		for k := range lastStreamIds1 {
			if lastStreamIds1[k] == lastStreamIds2[k] {
				u = false
				break
			}
		}
		fmt.Println(lastStreamIds2)
		fmt.Println("updated", u)
		return u
	}

	if block > 0 {
		time.Sleep(block)
		if !updated() {
			return nil
		}
		return s.readStreams(req)
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if updated() {
			return s.readStreams(req)
		}
		<-ticker.C
	}
}

func (s *Store) parseStreamId(id string) (int64, int, error) {
	parts := strings.Split(id, "-")
	var ms int64
	var seq int
	ms, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	if len(parts) == 0 {
		return ms, 0, nil
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
	d, err := pkg.ReadRDB(path)
	if err != nil {
		return err
	}

	for k, v := range d {
		s.store[k] = &Val{
			val:       &TypedValue{Type: "string", Val: v.Val.(string)},
			ex:        v.Expiry,
			canExpire: !v.Expiry.Equal(time.Time{}),
		}
	}
	return nil
}
