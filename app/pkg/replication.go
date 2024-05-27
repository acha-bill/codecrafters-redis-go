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
	Role   ReplicaType
	Of     string
	config Config
}

func NewReplica(role ReplicaType, of string, config Config) *Replica {
	return &Replica{
		Role:   role,
		Of:     of,
		config: config,
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

	ch := make(chan []byte, 1)
	defer close(ch)
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
	fmt.Println("wait for pong")
	res := <-ch
	fmt.Println("received ", string(res))
	if !bytes.Equal(res, resp.Pong) {
		return fmt.Errorf("expected pong. got %s", string(res))
	}

	fmt.Println("writing REPLCONF 1")
	_, err = conn.Write(resp.Encode([]string{"REPLCONF", "listening-port", strconv.Itoa(r.config.Port)}))
	if err != nil {
		return fmt.Errorf("write: %w", err)
	}

	fmt.Println("wait for ok")
	res = <-ch
	fmt.Println("received ", string(res))
	if !bytes.Equal(res, resp.Ok) {
		return fmt.Errorf("expected ok. got %s", string(res))
	}

	_, err = conn.Write(resp.Encode([]string{"REPLCONF", "capa", "psync2"}))
	if err != nil {
		return fmt.Errorf("write: %w", err)
	}

	fmt.Println("wait for ok")
	res = <-ch
	fmt.Println("received ", string(res))
	if !bytes.Equal(res, resp.Ok) {
		return fmt.Errorf("expected ok. got %s", string(res))
	}
	return nil
}
