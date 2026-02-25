package main

import (
	"testing"
)

func TestRDB(t *testing.T) {
	redis_config := RedisConfig{port: 6389}
	server := NewRedis(redis_config)
	server.rdb_dir = "/home/rabin/projects/codecrafters/codecrafters-redis-go/"
	server.dbfilename = "dump.rdb"

	server.readRDB()
	//todo: HandleKEYS(server, cmd)
}
