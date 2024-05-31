package handler

import (
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/pkg"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"github.com/codecrafters-io/redis-starter-go/app/store"
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
	store *store.Store
}
type setOpts struct {
	px time.Duration
}

func NewSet(s *store.Store) *Set {
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
	h.store.SetString(k, v, o.px)
	fmt.Printf("set %s:%s, store=%s\n", k, v, h.store.Print())
	res <- resp.Ok
	return nil
}

type Get struct {
	store *store.Store
}

func NewGet(s *store.Store) *Get {
	return &Get{store: s}
}
func (h *Get) Handle(sId int64, args []resp.Value, res chan<- []byte) error {
	if len(args) < 2 {
		return ErrInvalidCmd
	}
	k := args[1].Val.(string)
	var r []byte
	if v, ok := h.store.Get(k); ok {
		r = resp.Encode(v.Val)
	} else {
		r = resp.Nil
	}
	res <- r
	return nil
}

type Info struct {
	repl *pkg.Replication
}
type infoOpts struct {
	replication bool
}

func NewInfo(repl *pkg.Replication) Info {
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
	repl *pkg.Replication
	ack  *atomic.Int64
}
type replicaConfigOpts struct {
	listeningPort int
	capa          string
	getack        string
	ack           string
}

func NewReplicaConfig(repl *pkg.Replication, ack *atomic.Int64) ReplicaConfig {
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

	s, ok := h.repl.GetSlave(sId)
	if !ok {
		s = pkg.NewReplica(sId)
		h.repl.SetSlave(sId, s)
	}
	s.Conf = true

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
			s.Ack = v
		}
		return nil
	}

	res <- resp.Ok
	return nil
}

type Psync struct {
	repl *pkg.Replication
}

func NewPsync(repl *pkg.Replication) Psync {
	return Psync{repl: repl}
}

func (h Psync) Handle(sId int64, args []resp.Value, res chan<- []byte) error {
	if len(args) < 3 {
		return ErrInvalidCmd
	}

	s, ok := h.repl.GetSlave(sId)
	if !ok {
		s = pkg.NewReplica(sId)
		h.repl.SetSlave(sId, s)
	}

	s.Psync = true
	s.Handshake = s.Conf && s.Psync

	res <- resp.EncodeSimple(fmt.Sprintf("FULLRESYNC %s 0", h.repl.ID))
	time.Sleep(1 * time.Second)
	res <- resp.EncodeRDB()

	return nil
}

type Wait struct {
	repl *pkg.Replication
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
func NewWait(repl *pkg.Replication, ack *atomic.Int64) Wait {
	return Wait{repl: repl, ack: ack}
}
func (h Wait) Handle(sId int64, args []resp.Value, res chan<- []byte) error {
	opts, err := h.parse(args)
	if err != nil {
		return err
	}

	for _, s := range h.repl.GetSlaves() {
		b := resp.Encode([]string{"REPLCONF", "GETACK", "*"})
		s.Conn.Write(b)
	}

	//time.Sleep(1 * time.Second)

	checker := time.NewTicker(50 * time.Millisecond)
	timeout := time.After(opts.timeout)
	defer checker.Stop()

	var c int
L:
	for {
		c = 0
		for _, s := range h.repl.GetSlaves() {
			if int64(s.Ack) >= h.ack.Load() {
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

	res <- resp.Encode(c)
	return nil
}

type Conf struct {
	c pkg.Config
}

func NewConf(c pkg.Config) Conf {
	return Conf{c: c}
}
func (h Conf) Handle(sId int64, args []resp.Value, res chan<- []byte) error {
	if len(args) < 3 {
		return ErrInvalidCmd
	}

	var v string
	p := args[2].Val.(string)
	switch p {
	case "dir":
		v = h.c.DbDir
	case "dbfilename":
		v = h.c.DbFileName
	}

	res <- resp.Encode([]string{p, v})
	return nil
}

type Keys struct {
	conf pkg.Config
}

func NewKeys(c pkg.Config) Keys {
	return Keys{conf: c}
}
func (h Keys) Handle(sId int64, args []resp.Value, res chan<- []byte) error {
	r, err := pkg.ReadRDB(path.Join(h.conf.DbDir, h.conf.DbFileName))
	if err != nil {
		return err
	}

	var keys []string
	for k := range r {
		keys = append(keys, k)
	}
	res <- resp.Encode(keys)

	return nil
}

type Type struct {
	store *store.Store
}

func NewType(s *store.Store) Type {
	return Type{store: s}
}
func (h Type) Handle(sId int64, args []resp.Value, res chan<- []byte) error {
	if len(args) < 2 {
		return ErrInvalidCmd
	}
	key := args[1].Val.(string)
	v, ok := h.store.Get(key)
	if ok {
		res <- resp.EncodeSimple(v.Type)
	} else {
		res <- resp.EncodeSimple("none")
	}
	return nil
}

type Xadd struct {
	s *store.Store
}

func NewXadd(s *store.Store) Xadd {
	return Xadd{s: s}
}
func (h Xadd) Handle(sId int64, args []resp.Value, res chan<- []byte) error {
	if len(args) < 3 {
		return ErrInvalidCmd
	}
	k, id := args[1].Val.(string), args[2].Val.(string)

	data := make(map[string]string)
	for i := 3; i < len(args); i += 2 {
		if i+1 >= len(args) {
			return ErrInvalidCmd
		}
		data[args[i].Val.(string)] = args[i+1].Val.(string)
	}
	id, err := h.s.SetStream(k, id, data, 0)
	if err != nil {
		return err
	}
	res <- resp.Encode(id)
	return nil
}

type Xrange struct {
	s *store.Store
}

func NewXrange(s *store.Store) Xrange {
	return Xrange{s: s}
}
func (h Xrange) Handle(sId int64, args []resp.Value, res chan<- []byte) error {
	if len(args) < 4 {
		return ErrInvalidCmd
	}
	k, startId, endId := args[1].Val.(string), args[2].Val.(string), args[3].Val.(string)
	r := h.s.RangeStream(k, startId, endId)
	res <- resp.Encode(r)
	return nil
}

type Xread struct {
	s *store.Store
}

func NewXread(s *store.Store) Xread {
	return Xread{s: s}
}
func (h Xread) Handle(sId int64, args []resp.Value, res chan<- []byte) error {
	if len(args) < 4 {
		return ErrInvalidCmd
	}
	k, startId := args[1].Val.(string), args[2].Val.(string)
	r := h.s.ReadStream(k, startId)
	res <- resp.Encode(r)
	return nil
}
