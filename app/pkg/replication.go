package pkg

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
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
	sId   int64
	capa  map[string]string
	port  int
	conf  bool
	psync bool
	ready bool
	conn  net.Conn
}

type Replication struct {
	Role   ReplicaType
	Of     string
	ID     string
	config Config
	slaves map[int64]*Replica
}

func NewReplica(role ReplicaType, of string, config Config) *Replication {
	return &Replication{
		Role:   role,
		Of:     of,
		config: config,
		ID:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		slaves: make(map[int64]*Replica),
	}
}

func (r *Replication) Handshake() error {
	master := strings.Split(r.Of, " ")
	if len(master) < 2 {
		return ErrInvalidMaster
	}
	host := strings.TrimSpace(master[0])
	port, err := strconv.Atoi(strings.TrimSpace(master[1]))
	if err != nil {
		return fmt.Errorf("invalid port: %w", err)
	}

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return err
	}

	ch := make(chan []byte, 1)
	go func() {
		for {
			b := make([]byte, 1024)
			_, err := conn.Read(b)
			if err != nil && !errors.Is(err, io.EOF) {
				log.Println("read ", err.Error())
			}
			ch <- b
		}
	}()

	_, err = conn.Write(resp.Encode([]string{"PING"}))
	if err != nil {
		return fmt.Errorf("write ping: %w", err)
	}
	res := <-ch
	if !bytes.Equal(res[:len(resp.Pong)], resp.Pong) {
		return fmt.Errorf("expected PONG")
	}

	_, err = conn.Write(resp.Encode([]string{"REPLCONF", "listening-port", strconv.Itoa(r.config.Port)}))
	if err != nil {
		return fmt.Errorf("write: %w", err)
	}

	res = <-ch
	if !bytes.Equal(res[:len(resp.Ok)], resp.Ok) {
		return fmt.Errorf("expected ok. got %s", string(res))
	}

	_, err = conn.Write(resp.Encode([]string{"REPLCONF", "capa", "psync2"}))
	if err != nil {
		return fmt.Errorf("write: %w", err)
	}

	res = <-ch
	if !bytes.Equal(res[:len(resp.Ok)], resp.Ok) {
		return fmt.Errorf("expected ok. got %s", string(res))
	}

	_, err = conn.Write(resp.Encode([]string{"PSYNC", "?", "-1"}))
	if err != nil {
		return fmt.Errorf("write: %w", err)
	}
	res = <-ch
	return nil
}
