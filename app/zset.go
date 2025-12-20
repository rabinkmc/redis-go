package main

import (
	"fmt"
	"strconv"
	"strings"
)

func (entry *Entry) bsearch_znode(score float64) int {
	items := entry.zset.items
	left := 0
	right := len(items) - 1
	idx := -1
	for left <= right {
		m := left + (right-left)/2
		if items[m].score <= score {
			idx = m
			left = m + 1
		} else {
			right = m - 1
		}
	}
	return idx
}

func (entry *Entry) insert_znode(item Znode) {
	items := entry.zset.items
	idx := entry.bsearch_znode(item.score) + 1
	items = append(items, Znode{})
	copy(items[idx+1:], items[idx:])
	items[idx] = item
	entry.zset.pos[item.member] = idx
	entry.zset.items = items
}

func (server *Redis) handleZADD(client *Client, args []string) {
	key := args[0]
	score, _ := strconv.ParseFloat(args[1], 64)
	member := args[2]
	entry, _ := server.dict[key]
	if entry.zset == nil {
		entry.zset = &Zset{pos: make(map[string]int)}
	}
	if idx, exists := entry.zset.pos[member]; exists {
		entry.zset.items[idx].score = score
		server.dict[key] = entry
		client.conn.Write([]byte(resp_int(0)))
	} else {
		entry.insert_znode(Znode{member: member, score: score})
		server.dict[key] = entry
		client.conn.Write([]byte(resp_int(1)))
	}
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
