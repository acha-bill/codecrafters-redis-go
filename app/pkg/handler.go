package pkg

import (
	"errors"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

var (
	ErrInvalidCmd = errors.New("invalid cmd")
)

type Handler interface {
	Handle(sId int64, args []resp.Value, res chan<- []byte) error
}

type Ping struct{}

func (h Ping) Handle(sId int64, args []resp.Value, res chan<- []byte) error {
	res <- resp.Encode("PONG")
	return nil
}

type Echo struct{}

func (h Echo) Handle(sId int64, args []resp.Value, res chan<- []byte) error {
	if len(args) < 2 {
		return ErrInvalidCmd
	}
	v := args[1].Val.(string)
	res <- resp.Encode(v)
	return nil
}

type Set struct {
	store *Store
}
type setOpts struct {
	px time.Duration
}

func NewSet(s *Store) *Set {
	return &Set{store: s}
}
func (h *Set) parse(args []resp.Value) (*setOpts, error) {
	var o setOpts
	for i := 3; i < len(args); i++ {
		switch strings.ToUpper(args[i].Val.(string)) {
		case "PX":
			if i+1 < len(args) {
				v, err := strconv.Atoi(args[i+1].Val.(string))
				if err != nil {
					return nil, err
				}
				o.px = time.Duration(v) * time.Millisecond
				i++
			}
		}
	}
	return &o, nil
}

func (h *Set) Handle(sId int64, args []resp.Value, res chan<- []byte) error {
	if len(args) < 3 {
		return ErrInvalidCmd
	}
	k, v := args[1].Val.(string), args[2].Val.(string)
	o, err := h.parse(args)
	if err != nil {
		return err
	}
	h.store.Set(k, v, o.px)
	fmt.Printf("set %s:%s, store=%s\n", k, v, h.store.Print())
	res <- resp.Ok
	return nil
}

type Get struct {
	store *Store
}

func NewGet(s *Store) *Get {
	return &Get{store: s}
}
func (h *Get) Handle(sId int64, args []resp.Value, res chan<- []byte) error {
	if len(args) < 2 {
		return ErrInvalidCmd
	}
	k := args[1].Val.(string)
	var r []byte
	if v, ok := h.store.Get(k); ok {
		r = resp.Encode(v)
	} else {
		r = resp.Nil
	}
	res <- r
	return nil
}

type Info struct {
	repl *Replication
}
type infoOpts struct {
	replication bool
}

func NewInfo(repl *Replication) Info {
	return Info{repl: repl}
}

func (h Info) parse(args []resp.Value) (infoOpts, error) {
	var opts infoOpts
	if len(args) < 2 {
		return opts, nil
	}
	sec := strings.ToUpper(args[1].Val.(string))
	switch sec {
	case "REPLICATION":
		opts.replication = true
	}

	return opts, nil
}

func (h Info) Handle(sId int64, args []resp.Value, res chan<- []byte) error {
	_, err := h.parse(args)
	if err != nil {
		return err
	}

	m := map[string]any{
		"role":               h.repl.Role,
		"master_replid":      h.repl.ID,
		"master_repl_offset": 0,
	}
	r := ""
	for k, v := range m {
		r += fmt.Sprintf("%s:%v\r\n", k, v)
	}
	res <- resp.Encode(r)
	return nil
}

type ReplicaConfig struct {
	repl *Replication
	ack  *atomic.Int64
}
type replicaConfigOpts struct {
	listeningPort int
	capa          string
	getack        string
	ack           string
}

func NewReplicaConfig(repl *Replication, ack *atomic.Int64) ReplicaConfig {
	return ReplicaConfig{repl: repl, ack: ack}
}

func (h ReplicaConfig) parse(args []resp.Value) (replicaConfigOpts, error) {
	var opts replicaConfigOpts
	if len(args) < 3 {
		return opts, nil
	}
	sec := strings.ToLower(args[1].Val.(string))
	switch sec {
	case "listening-port":
		opts.listeningPort, _ = strconv.Atoi(args[2].Val.(string))
	case "capa":
		opts.capa = args[2].Val.(string)
	case "getack":
		opts.getack = args[2].Val.(string)
	case "ack":
		opts.ack = args[2].Val.(string)
	}

	return opts, nil
}
func (h ReplicaConfig) Handle(sId int64, args []resp.Value, res chan<- []byte) error {
	o, err := h.parse(args)
	if err != nil {
		return err
	}

	s, ok := h.repl.slaves[sId]
	if !ok {
		s = NewReplica(sId)
		h.repl.slaves[sId] = s
	}
	s.conf = true

	if o.getack != "" {
		ack := fmt.Sprintf("%d", h.ack.Load())
		res <- resp.Encode([]string{"REPLCONF", "ACK", ack})
		return nil
	}

	if o.ack != "" {
		fmt.Printf("ack res from sId=%d,: %+v\n", sId, args)
		v, err := strconv.Atoi(o.ack)
		if err != nil {
			fmt.Println("invalid number: ", err.Error())
		} else {
			s.ack = v
		}
		return nil
	}

	res <- resp.Ok
	return nil
}

type Psync struct {
	repl *Replication
}

func NewPsync(repl *Replication) Psync {
	return Psync{repl: repl}
}

func (h Psync) Handle(sId int64, args []resp.Value, res chan<- []byte) error {
	if len(args) < 3 {
		return ErrInvalidCmd
	}

	s, ok := h.repl.slaves[sId]
	if !ok {
		s = NewReplica(sId)
		h.repl.slaves[sId] = s
	}

	s.psync = true
	s.handshake = s.conf && s.psync

	res <- resp.EncodeSimple(fmt.Sprintf("FULLRESYNC %s 0", h.repl.ID))
	time.Sleep(1 * time.Second)
	res <- resp.EncodeRDB()

	return nil
}

type Wait struct {
	repl *Replication
	ack  *atomic.Int64
}
type waitOpts struct {
	replicas int
	timeout  time.Duration
}

func (h Wait) parse(args []resp.Value) (waitOpts, error) {
	var opts waitOpts
	if len(args) < 3 {
		return opts, nil
	}
	opts.replicas, _ = strconv.Atoi(args[1].Val.(string))
	tout, _ := strconv.Atoi(args[2].Val.(string))
	opts.timeout = time.Duration(tout) * time.Millisecond

	return opts, nil
}
func NewWait(repl *Replication, ack *atomic.Int64) Wait {
	return Wait{repl: repl, ack: ack}
}
func (h Wait) Handle(sId int64, args []resp.Value, res chan<- []byte) error {
	opts, err := h.parse(args)
	if err != nil {
		return err
	}

	for _, s := range h.repl.slaves {
		b := resp.Encode([]string{"REPLCONF", "GETACK", "*"})
		s.conn.Write(b)
	}

	//time.Sleep(1 * time.Second)

	checker := time.NewTicker(50 * time.Millisecond)
	timeout := time.After(opts.timeout)
	defer checker.Stop()

	var c int
L:
	for {
		c = 0
		for _, s := range h.repl.slaves {
			if int64(s.ack) >= h.ack.Load() {
				c++
			}
		}
		if c >= opts.replicas {
			break L
		}

		select {
		case <-timeout:
			break L
		case <-checker.C:
			continue
		}
	}

	for _, s := range h.repl.slaves {
		fmt.Printf("sid = %d, ack = %d, master ack=%d\n", s.sId, s.ack, h.ack.Load())
	}
	res <- resp.Encode(c)
	return nil
}
