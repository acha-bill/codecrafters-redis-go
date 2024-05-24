package main

import (
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/pkg"
	"log"
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

	store := pkg.NewStore()
	handlers := map[string]pkg.Handler{
		"PING": pkg.Ping{},
		"ECHO": pkg.Echo{},
		"SET":  pkg.NewSet(store),
		"GET":  pkg.NewGet(store),
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatal("Error accepting connection: ", err.Error())
		}

		session := pkg.NewSession(conn, handlers)
		go session.Start()
	}
}
