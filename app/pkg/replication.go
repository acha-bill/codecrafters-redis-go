package pkg

import (
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
	conn net.Conn
}

func NewReplica(role ReplicaType, of string) *Replica {
	return &Replica{
		Role: role,
		Of:   of,
	}
}

func (r *Replica) Handshake1() error {
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

	r.conn = conn
	return nil
}
