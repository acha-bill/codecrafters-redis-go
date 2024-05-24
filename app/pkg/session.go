package pkg

import (
	"errors"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"io"
	"log"
	"net"
	"strings"
)

// Session is the life cycle of a connection
type Session struct {
	conn     net.Conn
	handlers map[string]Handler
	inC      chan resp.Value
	outC     chan []byte
}

func NewSession(conn net.Conn, handlers map[string]Handler) *Session {
	return &Session{
		conn:     conn,
		handlers: handlers,
		inC:      make(chan resp.Value),
		outC:     make(chan []byte),
	}
}
func (s *Session) Start() {
	go s.readLoop()
	go s.writeLoop()
	go s.worker()
}

func (s *Session) Close() {
	s.conn.Close()
	close(s.inC)
	close(s.outC)
}

func (s *Session) worker() {
	for in := range s.inC {
		err := s.handle(in)
		if err != nil {
			log.Println("handle in", err.Error())
			continue
		}
	}
}

func (s *Session) handle(in resp.Value) error {
	if in.Type != resp.Array {
		return fmt.Errorf("only array allowed")
	}
	args := in.Val.([]resp.Value)
	if len(args) == 0 {
		return fmt.Errorf("command is missing")
	}
	if args[0].Type != resp.BulkString {
		return fmt.Errorf("bulk string expected")
	}
	cmd := strings.ToUpper(args[0].Val.(string))
	h, ok := s.handlers[cmd]
	if !ok {
		return fmt.Errorf("handler for cmd %s not found", cmd)
	}
	r, err := h.Handle(args)
	if err != nil {
		return err
	}
	s.outC <- r
	return nil
}

func (s *Session) writeLoop() {
	for d := range s.outC {
		_, err := s.conn.Write(d)
		if err != nil {
			log.Println("write to conn: ", err.Error())
		}
	}
}

func (s *Session) readLoop() {
	buf := make([]byte, 1024)
	for {
		_, err := s.conn.Read(buf)
		if errors.Is(err, io.EOF) {
			s.Close()
			return
		}
		if err != nil {
			log.Println("read: ", err.Error())
			continue
		}

		var val resp.Value
		_, err = resp.Decode(buf, &val)
		if err != nil {
			log.Println("decode input: ", err.Error())
			continue
		}

		s.inC <- val
	}
}
