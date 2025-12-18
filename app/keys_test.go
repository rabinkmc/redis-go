package main

import (
	"fmt"
	"testing"
)

func TestRDB(t *testing.T) {
	server := NewRedis(6389, "")
	server.rdb_dir = "/home/rabin/projects/codecrafters/codecrafters-redis-go/"
	server.dbfilename = "dump.rdb"

	got := server.handleKEYS()
	fmt.Printf("%#v", got)
}
