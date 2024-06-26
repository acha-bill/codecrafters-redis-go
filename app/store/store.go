package store

import (
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/pkg"
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

type streamDetail struct {
	c int
	r int
}
type Store struct {
	store map[string]*Val
	mu    sync.RWMutex

	streamDetails map[string]*streamDetail
}

func New() *Store {
	return &Store{
		store:         make(map[string]*Val),
		streamDetails: make(map[string]*streamDetail),
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
		s.streamDetails[k] = &streamDetail{c: 1}
		return id, nil
	}

	stream := storeVal.val.Val.(*Stream)
	stream.Entries = append(stream.Entries, StreamEntry{
		ID:     id,
		Values: data,
	})
	s.streamDetails[k].c++
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

func (s *Store) sanitizeReadStream(res []*ReadStreamRes, req [][]string, details map[string]int) []*ReadStreamRes {
	freshOnly := make(map[string]bool)
	for _, v := range req {
		freshOnly[v[0]] = false
		if v[1] == "$" {
			freshOnly[v[0]] = true
		}
	}

	for _, v := range res {
		if freshOnly[v.Stream] {
			v.Entries = v.Entries[details[v.Stream]:]

		}
	}
	return res
}

func (s *Store) ReadStream(req [][]string, block time.Duration) []*ReadStreamRes {
	res := s.readStreams(req)
	if block < 0 {
		return res
	}
	if res == nil {
		return nil
	}

	read0 := make(map[string]int)
	for _, v := range res {
		read0[v.Stream] = len(v.Entries)
	}

	updated := func() bool {
		read1 := make(map[string]int)
		for _, v := range res {
			read1[v.Stream] = len(v.Entries)
		}
		//fmt.Println(read0, read1)
		for k := range read0 {
			if read0[k] == read1[k] {
				return false
			}
		}
		return true
	}

	if block > 0 {
		time.Sleep(block)
		res = s.readStreams(req)
		if !updated() {
			return nil
		}
		return s.sanitizeReadStream(res, req, read0)
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		<-ticker.C
		res = s.readStreams(req)
		if !updated() {
			continue
		}
		return s.sanitizeReadStream(res, req, read0)
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
