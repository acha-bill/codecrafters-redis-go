package pkg

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
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

	ch chan []byte
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
	fmt.Println("pushing cmd to buf: ", string(b))
	r.ch <- b
}

func (r *Replica) Start() {
	for r.conn == nil {
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
			_, err := conn.Read(b)
			if err != nil && !errors.Is(err, io.EOF) {
				fmt.Println("read ", err.Error())
			}
			ch <- b
		}
	}()

	_, err = conn.Write(resp.Encode([]string{"PING"}))
	if err != nil {
		return nil, fmt.Errorf("write ping: %w", err)
	}
	res := <-ch
	if !bytes.Equal(res[:len(resp.Pong)], resp.Pong) {
		return nil, fmt.Errorf("expected PONG")
	}

	_, err = conn.Write(resp.Encode([]string{"REPLCONF", "listening-port", strconv.Itoa(r.config.Port)}))
	if err != nil {
		return nil, fmt.Errorf("write: %w", err)
	}

	res = <-ch
	if !bytes.Equal(res[:len(resp.Ok)], resp.Ok) {
		return nil, fmt.Errorf("expected ok. got %s", string(res))
	}

	_, err = conn.Write(resp.Encode([]string{"REPLCONF", "capa", "psync2"}))
	if err != nil {
		return nil, fmt.Errorf("write: %w", err)
	}

	res = <-ch
	if !bytes.Equal(res[:len(resp.Ok)], resp.Ok) {
		return nil, fmt.Errorf("expected ok. got %s", string(res))
	}

	_, err = conn.Write(resp.Encode([]string{"PSYNC", "?", "-1"}))
	if err != nil {
		return nil, fmt.Errorf("write: %w", err)
	}
	res = <-ch
	return conn, nil
}
