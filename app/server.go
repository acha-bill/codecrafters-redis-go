package main

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"io"
	"net"
	"os"
	"strings"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	for {
		buf := make([]byte, 64)
		_, err := conn.Read(buf)
		if errors.Is(err, io.EOF) {
			conn.Close()
			return
		}
		if err != nil {
			fmt.Println("read from connection: ", err.Error())
			os.Exit(1)
		}

		buf = bytes.TrimSpace(buf)
		var val resp.Value
		_, err = resp.Decode(buf, &val)
		if err != nil {
			fmt.Println("decode input ", err.Error())
			os.Exit(1)
		}

		err = handleInput(val, conn)
		if err != nil {
			fmt.Println("handle cmd:", err.Error())
			os.Exit(1)
		}
	}
}

func handleInput(in resp.Value, conn net.Conn) error {
	if in.Type != resp.Array {
		return fmt.Errorf("only array allowed")
	}
	arr := in.Val.([]resp.Value)
	if len(arr) == 0 {
		return fmt.Errorf("command is missing")
	}
	if arr[0].Type != resp.BulkString {
		return fmt.Errorf("bulk string expected")
	}
	cmd := strings.ToUpper(arr[0].Val.(string))
	switch cmd {
	case "PING":
		return handlePing(conn)
	case "ECHO":
		var v string
		if len(arr) > 0 {
			v = arr[1].Val.(string)
		}
		return handleEcho(v, conn)
	}

	return fmt.Errorf("invalid command: %s", cmd)
}

func handleEcho(v string, conn net.Conn) error {
	d := resp.Encode(v)
	_, err := conn.Write(d)
	return err
}

func handlePing(conn net.Conn) error {
	d := resp.Encode("PONG")
	_, err := conn.Write(d)
	return err
}
