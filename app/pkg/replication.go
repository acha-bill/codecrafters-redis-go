package pkg

import (
	"errors"
	"fmt"
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
	ack       int

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

func (r *Replication) Dial() (net.Conn, error) {
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
	return conn, nil
}
