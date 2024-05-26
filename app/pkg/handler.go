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
	Handle(args []resp.Value) ([]byte, error)
}

type Ping struct{}

func (h Ping) Handle(_ []resp.Value) ([]byte, error) {
	return resp.Encode("PONG"), nil
}

type Echo struct{}

func (h Echo) Handle(args []resp.Value) ([]byte, error) {
	if len(args) < 2 {
		return nil, ErrInvalidCmd
	}
	v := args[1].Val.(string)
	return resp.Encode(v), nil
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

func (h *Set) Handle(args []resp.Value) ([]byte, error) {
	if len(args) < 3 {
		return nil, ErrInvalidCmd
	}
	k, v := args[1].Val.(string), args[2].Val.(string)
	o, err := h.parse(args)
	if err != nil {
		return nil, err
	}

	h.store.Set(k, v, o.px)
	return resp.Ok, nil
}

type Get struct {
	store *Store
}

func NewGet(s *Store) *Get {
	return &Get{store: s}
}
func (h *Get) Handle(args []resp.Value) ([]byte, error) {
	if len(args) < 2 {
		return nil, ErrInvalidCmd
	}
	k := args[1].Val.(string)
	var r []byte
	if v, ok := h.store.Get(k); ok {
		r = resp.Encode(v)
	} else {
		r = resp.Nil
	}
	return r, nil
}

type Info struct {
	repl Replication
}
type infoOpts struct {
	replication bool
}

func NewInfo(repl Replication) Info {
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
func (h Info) Handle(args []resp.Value) ([]byte, error) {
	_, err := h.parse(args)
	if err != nil {
		return nil, err
	}

	return resp.Encode(fmt.Sprintf("role:%s", h.repl.Role)), nil
}
