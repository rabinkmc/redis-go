package main

import (
	"strconv"
	"strings"
)

func (server *Redis) handleZADD(client *Client, args []string) {
	key := args[0]
	score, _ := strconv.ParseFloat(args[1], 64)
	member := args[2]
	entry, _ := server.dict[key]
	entry.zset = append(entry.zset, Znode{member: member, score: score})
	server.dict[key] = entry
	response := resp_int(len(entry.zset))
	client.conn.Write([]byte(response))
}

func is_set_cmd(cmd string) bool {
	commands := []string{"ZADD"}
	for _, command := range commands {
		if cmd == command {
			return true
		}
	}
	return false
}

func (server *Redis) handleZset(client *Client, args []string) {
	cmd := strings.ToUpper(args[0])
	switch cmd {
	case "ZADD":
		server.handleZADD(client, args[1:])
	}

}
