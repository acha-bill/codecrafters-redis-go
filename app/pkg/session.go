package pkg

import (
	"errors"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"io"
	"net"
	"time"
)

// Session is the life cycle of a connection
type Session struct {
	conn       net.Conn
	handlers   map[string]Handler
	inC        chan resp.Value
	outC       chan []byte
	repl       *Replication
	id         int64
	responsive bool
}

func NewSession(conn net.Conn, handlers map[string]Handler, repl *Replication) *Session {
	return &Session{
		conn:       conn,
		handlers:   handlers,
		inC:        make(chan resp.Value),
		outC:       make(chan []byte),
		repl:       repl,
		id:         time.Now().UnixNano(),
		responsive: true,
	}
}

func (s *Session) Responsive(v bool) *Session {
	s.responsive = v
	return s
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
			fmt.Println("handle input", in, err.Error())
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
	fmt.Printf("handling %s cmd with args %+v\n", cmd, args)

	res := make(chan []byte)
	defer close(res)
	go func() {
		for r := range res {
			if s.responsive {
				s.outC <- r
			}
		}
	}()

	err = h.Handle(s.id, args, res)
	if err != nil {
		return err
	}

	// setup slave conn
	sl, ok := s.repl.slaves[s.id]
	if ok && sl.handshake && sl.conn == nil {
		sl.conn = s.conn
	}

	return nil
}

func (s *Session) writeLoop() {
	for d := range s.outC {
		_, err := s.conn.Write(d)
		if err != nil {
			fmt.Println("write to conn: ", err.Error())
		}
	}
}

func (s *Session) readLoop() {
	buf := make([]byte, 1024)
	for {
		n, err := s.conn.Read(buf)
		if errors.Is(err, io.EOF) {
			s.Close()
			return
		}
		if err != nil {
			fmt.Println("read: ", err.Error())
			continue
		}
		buf = buf[:n]
		fmt.Printf("read: %q\n", string(buf))
		var val resp.Value
		_, err = resp.Decode(buf, &val)
		if err != nil {
			fmt.Println("decode input: ", err.Error())
			continue
		}

		// propagate write commands to slaves
		if s.repl.Role == MasterReplica {
			cmd, _, err := resp.DecodeCmd(val)
			if err != nil {
				fmt.Println("decode cmd: ", err.Error())
				return
			}
			if cmd != "SET" {
				return
			}

			fmt.Printf("replicate: %q\n", string(buf))
			for _, sl := range s.repl.slaves {
				sl.Push(buf)
			}
		}

		s.inC <- val
	}
}
