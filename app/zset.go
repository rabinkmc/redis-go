package main

import (
	"sort"
	"strconv"
	"strings"
)

func (entry *Entry) find_znode(member string) int {
	items := entry.zset
	for idx, item := range items {
		if item.member == member {
			return idx
		}
	}
	return -1
}

func (entry *Entry) insert_znode(item Znode) {
	entry.zset = append(entry.zset, item)
	sort.Slice(entry.zset, func(i, j int) bool {
		items := entry.zset
		if items[i].score == items[j].score {
			return items[i].member < items[j].member
		}
		return items[i].score < items[j].score
	})
}

func (server *Redis) handleZADD(client *Client, args []string) {
	key := args[0]
	score, _ := strconv.ParseFloat(args[1], 64)
	member := args[2]
	entry, _ := server.dict[key]
	if idx := entry.find_znode(member); idx != -1 {
		entry.zset[idx].score = score
		server.dict[key] = entry
		client.conn.Write([]byte(resp_int(0)))
	} else {
		entry.insert_znode(Znode{member: member, score: score})
		server.dict[key] = entry
		client.conn.Write([]byte(resp_int(1)))
	}
}

func (server *Redis) handleZRANK(client *Client, args []string) {
	key := args[0]
	member := args[1]
	entry, _ := server.dict[key]
	NULL_BULKSTRING := "$-1\r\n"
	if len(entry.zset) == 0 {
		client.conn.Write([]byte(NULL_BULKSTRING))
		return
	}
	if idx := entry.find_znode(member); idx == -1 {
		client.conn.Write([]byte(NULL_BULKSTRING))
	} else {
		client.conn.Write([]byte(resp_int(idx)))
	}
}

func is_set_cmd(cmd string) bool {
	commands := []string{"ZADD", "ZRANK"}
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
	case "ZRANK":
		server.handleZRANK(client, args[1:])
	}
}
