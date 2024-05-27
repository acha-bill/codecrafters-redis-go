package pkg

import (
	"errors"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"io"
	"log"
	"net"
	"time"
)

// Session is the life cycle of a connection
type Session struct {
	conn     net.Conn
	handlers map[string]Handler
	inC      chan resp.Value
	outC     chan []byte
	repl     *Replication
	id       int64
}

func NewSession(conn net.Conn, handlers map[string]Handler, repl *Replication) *Session {
	return &Session{
		conn:     conn,
		handlers: handlers,
		inC:      make(chan resp.Value),
		outC:     make(chan []byte),
		repl:     repl,
		id:       time.Now().UnixNano(),
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
			log.Println("handle input", in, err.Error())
			continue
		}
	}
}

func (s *Session) handle(in resp.Value) error {
	cmd, args, err := resp.DecodeCmd(in)
	if err != nil {
		return err
	}
	h, ok := s.handlers[cmd]
	if !ok {
		return fmt.Errorf("handler for cmd %s not found", cmd)
	}
	log.Printf("handling %s cmd with args %+v", cmd, args)

	res := make(chan []byte)
	defer close(res)
	go func() {
		for r := range res {
			s.outC <- r
		}
	}()

	err = h.Handle(s.id, args, res)
	if err != nil {
		return err
	}

	// setup slave conn
	sl, ok := s.repl.slaves[s.id]
	if ok && sl.ready && sl.conn == nil {
		sl.conn = s.conn
	}

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

		go func(buf []byte) {
			// propagate write commands to slaves
			cmd, _, err := resp.DecodeCmd(val)
			if err != nil {
				fmt.Println("decode cmd: ", err.Error())
				return
			}
			if cmd != "SET" {
				return
			}

			fmt.Println("propagating cmd: ", cmd)
			for _, sl := range s.repl.slaves {
				if sl.conn != nil {
					sl.conn.Write(buf)
				}
			}
		}(buf[:])

		s.inC <- val
	}
}
