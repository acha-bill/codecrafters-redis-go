package pkg

import (
	"bytes"
	"errors"
	"fmt"
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
	Role ReplicaType
	Of   string
}

func NewReplica(role ReplicaType, of string) *Replica {
	return &Replica{
		Role: role,
		Of:   of,
	}
}

func (r *Replica) Handshake() error {
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

	_, err = conn.Write(resp.Encode([]string{"PING"}))
	if err != nil {
		return fmt.Errorf("write ping: %w", err)
	}

	err = r.waitOk(conn)
	if err != nil {
		return err
	}

	_, err = conn.Write(resp.Encode([]string{"REPLCONF", "listening-port", "xxx"}))
	if err != nil {
		return fmt.Errorf("write ping: %w", err)
	}
	err = r.waitOk(conn)
	if err != nil {
		return err
	}

	_, err = conn.Write(resp.Encode([]string{"REPLCONF", "capa", "psync2"}))
	if err != nil {
		return fmt.Errorf("write ping: %w", err)
	}
	err = r.waitOk(conn)
	if err != nil {
		return err
	}
	return nil
}

func (r *Replica) waitOk(conn net.Conn) error {
	b := make([]byte, len(resp.Ok))
	_, err := conn.Read(b)
	if err != nil {
		return err
	}
	if bytes.Equal(b, resp.Ok) {
		return nil
	}
	return fmt.Errorf("invalid response from master: %s", string(b))
}
