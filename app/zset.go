package main

import (
	"fmt"
	"sort"
	"strconv"
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
func (entry *Entry) sort_zset() {
	sort.Slice(entry.zset, func(i, j int) bool {
		items := entry.zset
		if items[i].score == items[j].score {
			return items[i].member < items[j].member
		}
		return items[i].score < items[j].score
	})
}

func (entry *Entry) insert_znode(item Znode) {
	entry.zset = append(entry.zset, item)
	entry.sort_zset()
}

func (entry *Entry) add_znode(item Znode) int {
	if idx := entry.find_znode(item.member); idx != -1 {
		entry.zset[idx].score = item.score
		entry.sort_zset()
		return 0
	} else {
		entry.insert_znode(item)
		return 1
	}
}

func HandleZADD(server *Redis, cmd Command) {
	server.mu.Lock()
	defer server.mu.Unlock()
	if len(cmd.Args) < 4 {
		cmd.Client.WriteErr("Invalid usage: ZADD key score member")
		return
	}
	client := cmd.Client
	key := cmd.Args[1]
	score, err := strconv.ParseFloat(cmd.Args[2], 64)
	if err != nil {
		client.conn.Write([]byte(
			simple_err(fmt.Sprintf("'%s' can't be converted to float", cmd.Args[2])),
		))
		return
	}
	member := cmd.Args[3]
	entry, _ := server.dict[key]
	znode := Znode{member: member, score: score}
	rv := entry.add_znode(znode)
	server.dict[key] = entry
	cmd.Client.WriteInt(rv)
}

func HandleZRANK(server *Redis, cmd Command) {
	server.mu.Lock()
	defer server.mu.Unlock()

	if len(cmd.Args) < 3 {
		cmd.Client.WriteErr("Invalid usage: ZRANK key member")
		return
	}
	key := cmd.Args[1]
	member := cmd.Args[2]
	entry, _ := server.dict[key]
	if len(entry.zset) == 0 {
		cmd.Client.WriteNil()
		return
	}
	if idx := entry.find_znode(member); idx == -1 {
		cmd.Client.WriteNil()
	} else {
		cmd.Client.WriteInt(idx)
	}
}

func HandleZRANGE(server *Redis, cmd Command) {
	server.mu.Lock()
	defer server.mu.Unlock()

	if len(cmd.Args) < 4 {
		cmd.Client.WriteErr("Invaid usage: ZRANGE key start end")
	}
	client := cmd.Client
	key := cmd.Args[1]
	entry, _ := server.dict[key]
	if len(entry.zset) == 0 {
		client.conn.Write([]byte(EMPTY_ARRAY))
		return
	}
	start, _ := strconv.ParseInt(cmd.Args[2], 10, 64)
	end, _ := strconv.ParseInt(cmd.Args[3], 10, 64)
	n := int64(len(entry.zset))
	if start < 0 {
		if -start >= n {
			start = 0
		} else {
			start = n + start
		}
	}
	if end < 0 {
		if -end >= n {
			end = 0
		}
		end = n + end
	}
	end = min(n-1, end)
	if start > end || start >= n {
		client.conn.Write([]byte(EMPTY_ARRAY))
		return
	}
	resp := []string{}
	for i := start; i <= end; i++ {
		resp = append(resp, entry.zset[i].member)
	}
	cmd.Client.WriteList(resp)
}

func HandleZCARD(server *Redis, cmd Command) {
	server.mu.Lock()
	defer server.mu.Unlock()

	if len(cmd.Args) < 2 {
		cmd.Client.WriteErr("Invalid usage:\nZCARD key")
		return
	}
	key := cmd.Args[1]
	entry, _ := server.dict[key]
	cmd.Client.WriteInt(len(entry.zset))
}

func HandleZSCORE(server *Redis, cmd Command) {
	server.mu.Lock()
	defer server.mu.Unlock()

	if len(cmd.Args) < 3 {
		cmd.Client.WriteErr("Invalid usage:\nZSCORE key member")
		return
	}
	key := cmd.Args[1]
	entry, _ := server.dict[key]
	if len(entry.zset) == 0 {
		cmd.Client.WriteNil()
		return
	}
	if idx := entry.find_znode(cmd.Args[2]); idx != -1 {
		s := strconv.FormatFloat(entry.zset[idx].score, 'f', -1, 64)
		cmd.Client.WriteBulkString(s)
	} else {
		cmd.Client.WriteNil()
	}

}

func HandleZREM(server *Redis, cmd Command) {
	server.mu.Lock()
	defer server.mu.Unlock()

	key := cmd.Args[1]
	if len(cmd.Args) < 3 {
		cmd.Client.WriteErr("Invalid usage: ZRANK key member")
		return
	}
	member := cmd.Args[2]
	entry, _ := server.dict[key]
	if len(entry.zset) == 0 {
		cmd.Client.WriteInt(0)
		return
	}
	idx := entry.find_znode(member)
	if idx == -1 {
		cmd.Client.WriteInt(0)
		return
	}
	items := entry.zset[:0]
	for i := 0; i < len(entry.zset); i++ {
		if i == idx {
			continue
		}
		items = append(items, entry.zset[i])
	}
	entry.zset = items
	server.dict[key] = entry
	cmd.Client.WriteInt(1)
}
