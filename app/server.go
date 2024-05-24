package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
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

		go handleConnection(err, conn)
	}
}

func handleConnection(err error, conn net.Conn) {
	for {
		buf := make([]byte, 1024)
		_, err = conn.Read(buf)
		if err != nil && !errors.Is(err, io.EOF) {
			fmt.Println("read from connection: ", err.Error())
			os.Exit(1)
		}

		_, err = conn.Write([]byte(("+PONG\r\n")))
		if err != nil {
			fmt.Println("write to connection: ", err.Error())
			os.Exit(1)
		}
	}
}
