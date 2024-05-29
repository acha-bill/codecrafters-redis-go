package main

import (
	"flag"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/pkg"
	"log"
	"net"
	"os"
)

var (
	port      int
	replicaOf string
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	flag.IntVar(&port, "port", 6379, "port number")
	flag.StringVar(&replicaOf, "replicaof", "", "the master to follow")
	flag.Parse()

	config := pkg.Config{Port: port}

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", config.Port))
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	store := pkg.NewStore()
	role := pkg.MasterReplica
	if replicaOf != "" {
		role = pkg.SlaveReplica
	}
	repl := pkg.NewReplication(role, replicaOf, config)
	handlers := map[string]pkg.Handler{
		"PING":     pkg.Ping{},
		"ECHO":     pkg.Echo{},
		"SET":      pkg.NewSet(store),
		"GET":      pkg.NewGet(store),
		"INFO":     pkg.NewInfo(repl),
		"REPLCONF": pkg.NewReplicaConfig(repl),
		"PSYNC":    pkg.NewPsync(repl),
	}

	if role == pkg.SlaveReplica {
		go func() {
			stop := make(chan any)
			conn, err := repl.Handshake(stop)
			if err != nil {
				fmt.Println("handshake with master: ", err.Error())
				os.Exit(1)
			}
			stop <- 1
			session := pkg.NewSession(conn, handlers, repl).Responsive(false)
			go session.Start()
		}()
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatal("Error accepting connection: ", err.Error())
		}

		session := pkg.NewSession(conn, handlers, repl)
		go session.Start()
	}
}
