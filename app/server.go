package main

import (
	"flag"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/handler"
	"github.com/codecrafters-io/redis-starter-go/app/pkg"
	"github.com/codecrafters-io/redis-starter-go/app/session"
	"github.com/codecrafters-io/redis-starter-go/app/store"
	"log"
	"net"
	"os"
	"path"
	"sync/atomic"
)

var (
	port       int
	replicaOf  string
	dbDir      string
	dbFileName string
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	flag.IntVar(&port, "port", 6379, "port number")
	flag.StringVar(&replicaOf, "replicaof", "", "the master to follow")
	flag.StringVar(&dbDir, "dir", "./", "db dir")
	flag.StringVar(&dbFileName, "dbfilename", "dump.rdb", "db file name")
	flag.Parse()

	config := pkg.Config{Port: port, DbFileName: dbFileName, DbDir: dbDir}

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", config.Port))
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	store := store.New()
	err = store.Load(path.Join(config.DbDir, config.DbFileName))
	if err != nil {
		fmt.Println("load rdb", err.Error())
		os.Exit(1)
	}

	role := pkg.MasterReplica
	if replicaOf != "" {
		role = pkg.SlaveReplica
	}
	repl := pkg.NewReplication(role, replicaOf, config)
	handlers := map[string]handler.Handler{
		"PING":   handler.Ping{},
		"ECHO":   handler.Echo{},
		"SET":    handler.NewSet(store),
		"GET":    handler.NewGet(store),
		"INFO":   handler.NewInfo(repl),
		"PSYNC":  handler.NewPsync(repl),
		"CONFIG": handler.NewConf(config),
		"KEYS":   handler.NewKeys(config),
		"TYPE":   handler.NewType(store),
		"XADD":   handler.NewXadd(store),
		"XRANGE": handler.NewXrange(store),
		"XREAD":  handler.NewXread(store),
	}

	ack0, ack1 := &atomic.Int64{}, &atomic.Int64{}

	if role == pkg.SlaveReplica {
		go func() {
			conn, err := repl.Dial()
			if err != nil {
				fmt.Println("dial master: ", err.Error())
				os.Exit(1)
			}

			handlers["REPLCONF"] = handler.NewReplicaConfig(repl, ack0)
			s := session.New(conn, handlers, repl, config, ack0).Responsive(false).Handshake(true)
			go s.Start()
		}()
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatal("Error accepting connection: ", err.Error())
		}

		handlers["REPLCONF"] = handler.NewReplicaConfig(repl, ack1)
		handlers["WAIT"] = handler.NewWait(repl, ack1)
		s := session.New(conn, handlers, repl, config, ack1)
		go s.Start()
	}
}
