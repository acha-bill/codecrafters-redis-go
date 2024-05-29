package pkg

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"io"
	"net"
	"strconv"
	"strings"
)

type ReplicaType string

const (
	MasterReplica ReplicaType = "master"
	SlaveReplica  ReplicaType = "slave"
)

var (
	ErrInvalidMaster = errors.New("invalid master")
)

type Replica struct {
	sId       int64
	capa      map[string]string
	port      int
	conf      bool
	psync     bool
	handshake bool
	conn      net.Conn

	ch   chan []byte
	cmds []string
}

func NewReplica(id int64) *Replica {
	r := &Replica{
		sId: id,
		ch:  make(chan []byte, 1024),
	}

	go r.Start()
	return r
}

func (r *Replica) Push(b []byte) {
	r.cmds = append(r.cmds, fmt.Sprintf("%q", string(b)))
	r.ch <- b
}

func (r *Replica) Start() {
	for r.conn == nil {
	}

	fmt.Println("replica is ready. pending commands...")
	for _, v := range r.cmds {
		fmt.Println(v)
	}

	for v := range r.ch {
		_, err := r.conn.Write(v)
		if err != nil {
			fmt.Println("write to slave: ", err.Error())
		}
	}
}

type Replication struct {
	Role   ReplicaType
	Of     string
	ID     string
	config Config
	slaves map[int64]*Replica
}

func NewReplication(role ReplicaType, of string, config Config) *Replication {
	return &Replication{
		Role:   role,
		Of:     of,
		config: config,
		ID:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		slaves: make(map[int64]*Replica),
	}
}

func (r *Replication) Handshake() (net.Conn, error) {
	master := strings.Split(r.Of, " ")
	if len(master) < 2 {
		return nil, ErrInvalidMaster
	}
	host := strings.TrimSpace(master[0])
	port, err := strconv.Atoi(strings.TrimSpace(master[1]))
	if err != nil {
		return nil, fmt.Errorf("invalid port: %w", err)
	}

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return nil, err
	}

	ch := make(chan []byte, 1)
	go func() {
		for {
			b := make([]byte, 1024)
			n, err := conn.Read(b)
			if err != nil && !errors.Is(err, io.EOF) {
				fmt.Println("read ", err.Error())
			}
			b = b[:n]
			fmt.Printf("read: %q\n", string(b))
			ch <- b
		}
	}()

	_, err = conn.Write(resp.Encode([]string{"PING"}))
	if err != nil {
		return nil, fmt.Errorf("write ping: %w", err)
	}
	res := <-ch
	fmt.Printf("got %q, want %q\n", string(res), string(resp.Encode("PONG")))
	if !bytes.Equal(res, resp.Encode("PONG")) {
		return nil, fmt.Errorf("expected PONG")
	}

	s := resp.Encode([]string{"REPLCONF", "listening-port", strconv.Itoa(r.config.Port)})
	fmt.Printf("sending: %q\n", string(s))
	_, err = conn.Write(s)
	if err != nil {
		return nil, fmt.Errorf("write: %w", err)
	}

	res = <-ch
	if !bytes.Equal(res, resp.Encode("OK")) {
		return nil, fmt.Errorf("expected ok. got %s", string(res))
	}

	_, err = conn.Write(resp.Encode([]string{"REPLCONF", "capa", "psync2"}))
	if err != nil {
		return nil, fmt.Errorf("write: %w", err)
	}

	res = <-ch
	if !bytes.Equal(res, resp.Encode("PONG")) {
		return nil, fmt.Errorf("expected ok. got %s", string(res))
	}

	_, err = conn.Write(resp.Encode([]string{"PSYNC", "?", "-1"}))
	if err != nil {
		return nil, fmt.Errorf("write: %w", err)
	}
	res = <-ch
	return conn, nil
}
