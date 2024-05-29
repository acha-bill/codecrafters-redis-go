package pkg

import (
	"errors"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"io"
	"net"
	"strconv"
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
	config     Config

	// handshake
	shouldHandshake  bool
	handshaking      bool
	handshakeStepper chan any
	handshakeCmd     string
}

func NewSession(conn net.Conn, handlers map[string]Handler, repl *Replication, config Config) *Session {
	return &Session{
		conn:             conn,
		handlers:         handlers,
		inC:              make(chan resp.Value),
		outC:             make(chan []byte),
		repl:             repl,
		id:               time.Now().UnixNano(),
		responsive:       true,
		config:           config,
		handshakeStepper: make(chan any),
	}
}

func (s *Session) Responsive(v bool) *Session {
	s.responsive = v
	return s
}
func (s *Session) Handshake(v bool) *Session {
	s.shouldHandshake = v
	s.handshaking = true
	return s
}

func (s *Session) Start() {
	go s.worker()
	go s.readLoop()
	go s.writeLoop()

	if s.repl.Role == SlaveReplica && s.shouldHandshake {
		go s.handshake()
	}
}

func (s *Session) handshake() {
	handshakeCmds := [][]string{
		{"PING"},
		{"REPLCONF", "listening-port", strconv.Itoa(s.config.Port)},
		{"REPLCONF", "capa", "psync2"},
		{"PSYNC", "?", "-1"},
	}
	for _, cmd := range handshakeCmds {
		s.handshakeCmd = cmd[0]
		s.conn.Write(resp.Encode(cmd))
		<-s.handshakeStepper
	}
	s.handshaking = false
}

func (s *Session) handleHandshakeRes(v resp.Value) {
	r := v.Val.(string)
	if (s.handshakeCmd == "PING" && r == "PONG") ||
		(s.handshakeCmd == "REPLCONF" && r == "OK") ||
		(s.handshakeCmd == "PSYNC") {
		s.handshakeStepper <- 1
	}
}

func (s *Session) Close() {
	s.conn.Close()
	close(s.inC)
	close(s.outC)
}

func (s *Session) worker() {
	for in := range s.inC {
		if s.handshaking {
			s.handleHandshakeRes(in)
			continue
		}
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

	res := make(chan []byte)
	defer close(res)

	mustRespond := map[string]any{
		"REPLCONF": 1,
	}
	go func() {
		for r := range res {
			_, ok := mustRespond[cmd]
			if s.responsive || ok {
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
	for {
		buf := make([]byte, 1024)
		n, err := s.conn.Read(buf)
		if errors.Is(err, io.EOF) {
			s.Close()
			return
		}
		if err != nil {
			fmt.Println("session read: ", err.Error())
			continue
		}
		buf = buf[:n]

		var bufs [][]byte
		var vals []resp.Value

		for len(buf) > 0 {
			var val resp.Value
			n1, err := resp.Decode(buf, &val)
			buf0, buf1 := buf[0:n1], buf[n1:]
			if err != nil {
				fmt.Printf("decode input: %q: %s\n", string(buf0), err.Error())
				break
			}
			bufs = append(bufs, buf0)
			vals = append(vals, val)
			buf = buf1
		}

		if len(bufs) != len(vals) {
			fmt.Println("len mismatch. want ", len(bufs), " got ", len(vals))
		}
		for i := range bufs {
			s.push(bufs[i], vals[i])
			s.inC <- vals[i]
		}
	}
}

func (s *Session) push(buf []byte, val resp.Value) {
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
}
