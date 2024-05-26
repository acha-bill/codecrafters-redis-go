package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/pkg"
)

var (
	port      int
	replicaOf string
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	flag.IntVar(&port, "port", 6379, "port number")
	flag.StringVar(&replicaOf, "replicaof", "", "the master to follow")
	flag.Parse()

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	store := pkg.NewStore()
	role := pkg.MasterReplica
	if replicaOf != "" {
		role = pkg.SlaveReplica
	}
	repl := pkg.NewReplica(role, replicaOf)
	handlers := map[string]pkg.Handler{
		"PING": pkg.Ping{},
		"ECHO": pkg.Echo{},
		"SET":  pkg.NewSet(store),
		"GET":  pkg.NewGet(store),
		"INFO": pkg.NewInfo(repl),
	}

	if role == pkg.SlaveReplica {
		err = repl.Handshake1()
		if err != nil {
			log.Fatal("handshake with master")
		}
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
