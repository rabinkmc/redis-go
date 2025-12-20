package main

import (
	"fmt"
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
	score, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		client.conn.Write([]byte(
			simple_err(fmt.Sprintf("'%s' can't be converted to float", args[1])),
		))
		return
	}
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

func (server *Redis) handleZRANGE(client *Client, args []string) {
	key := args[0]
	entry, _ := server.dict[key]
	if len(entry.zset) == 0 {
		client.conn.Write([]byte(EMPTY_ARRAY))
	}
	start, _ := strconv.ParseInt(args[1], 10, 64)
	end, _ := strconv.ParseInt(args[2], 10, 64)
	n := int64(len(entry.zset) - 1)
	end = min(n, end)
	if start > end || start > n {
		client.conn.Write([]byte(EMPTY_ARRAY))
	}
	resp := []string{}
	for i := start; i <= end; i++ {
		resp = append(resp, entry.zset[i].member)
	}
	client.conn.Write([]byte(encode_list(resp)))
}

func is_set_cmd(cmd string) bool {
	commands := []string{"ZADD", "ZRANK", "ZRANGE"}
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
	case "ZRANGE":
		server.handleZRANGE(client, args[1:])
	}
}
