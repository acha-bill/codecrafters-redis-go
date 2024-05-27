package pkg

import (
	"errors"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"strconv"
	"strings"
	"time"
)

var (
	ErrInvalidCmd = errors.New("invalid cmd")
)

type Handler interface {
	Handle(args []resp.Value, res chan<- []byte) error
}

type Ping struct{}

func (h Ping) Handle(args []resp.Value, res chan<- []byte) error {
	res <- resp.Encode("PONG")
	return nil
}

type Echo struct{}

func (h Echo) Handle(args []resp.Value, res chan<- []byte) error {
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

func (h *Set) Handle(args []resp.Value, res chan<- []byte) error {
	if len(args) < 3 {
		return ErrInvalidCmd
	}
	k, v := args[1].Val.(string), args[2].Val.(string)
	o, err := h.parse(args)
	if err != nil {
		return err
	}
	h.store.Set(k, v, o.px)
	res <- resp.Ok
	return nil
}

type Get struct {
	store *Store
}

func NewGet(s *Store) *Get {
	return &Get{store: s}
}
func (h *Get) Handle(args []resp.Value, res chan<- []byte) error {
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
	repl *Replica
}
type infoOpts struct {
	replication bool
}

func NewInfo(repl *Replica) Info {
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

func (h Info) Handle(args []resp.Value, res chan<- []byte) error {
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

type ReplicaConfig struct{}
type replicaConfigOpts struct {
	listeningPort int
	capa          string
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
	}

	return opts, nil
}
func (h ReplicaConfig) Handle(args []resp.Value, res chan<- []byte) error {
	_, err := h.parse(args)
	if err != nil {
		return err
	}
	res <- resp.Ok
	return nil
}

type Psync struct {
	repl *Replica
}

func NewPsync(repl *Replica) Psync {
	return Psync{repl: repl}
}

func (h Psync) Handle(args []resp.Value, res chan<- []byte) error {
	if len(args) < 3 {
		return ErrInvalidCmd
	}
	res <- resp.EncodeSimple(fmt.Sprintf("FULLRESYNC %s 0", h.repl.ID))
	rdb := resp.EncodeRDB()
	fmt.Println("rdb: ", rdb)
	fmt.Println("rdb string: ", string(rdb))
	res <- rdb
	return nil
}
