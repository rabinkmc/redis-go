package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
)

func main() {
	port := flag.Int("port", 6379, "port to listen on")
	replicaof := flag.String("replicaof", "", "host and port of master")
	rdb_dir := flag.String("dir", "", "directory of rdb file")
	dbfilename := flag.String("dbfilename", "", "file name of rdb")
	flag.Parse()
	redis_config := RedisConfig{
		port:       *port,
		replicaof:  *replicaof,
		rdb_dir:    *rdb_dir,
		dbfilename: *dbfilename,
	}
	server := NewRedis(redis_config)

	address := fmt.Sprintf("0.0.0.0:%d", redis_config.port)
	l, err := net.Listen("tcp", address)
	if err != nil {
		log.Printf("Failed to bind to port %d\n", port)
		os.Exit(1)
	}
	log.Println("Server running at: ", address)
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatalf("Error accepting connection: %v", err)
		}
		go server.HandleConnection(conn)
	}
}
