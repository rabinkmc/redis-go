package main

import (
	"strconv"
	"time"
)

func HandleRPUSH(server *Redis, cmd Command) {
	server.mu.Lock()
	defer server.mu.Unlock()

	if len(cmd.Args) < 3 {
		cmd.Client.WriteErr(
			"Not enough args for RPUSH: RPUSH key val [val...]",
		)
		return
	}
	key := cmd.Args[1]
	values := cmd.Args[2:]

	entry := server.dict[key]
	entry.list = append(entry.list, values...)
	server.dict[key] = entry
	n := len(entry.list)

	if ch, exists := server.waiters[key]; exists {
		val := entry.list[0]
		entry.list = entry.list[1:]

		server.dict[key] = entry

		delete(server.waiters, key)

		go func(c chan string, v string) {
			c <- v
		}(ch, val)
	}
	cmd.Client.WriteInt(n)
}

func HandleLPUSH(server *Redis, cmd Command) {
	server.mu.Lock()
	defer server.mu.Unlock()

	if len(cmd.Args) < 3 {
		cmd.Client.WriteErr(
			"Not enough args for LPUSH: LPUSH key val [val...]",
		)
		return
	}
	key := cmd.Args[1]
	values := cmd.Args[2:]
	entry := server.dict[key]
	items := []string{}
	for i := len(values) - 1; i > -1; i-- {
		items = append(items, values[i])
	}
	entry.list = append(items, entry.list...)
	server.dict[key] = entry

	if ch, exists := server.waiters[key]; exists {
		val := entry.list[0]
		entry.list = entry.list[1:]
		server.dict[key] = entry
		delete(server.waiters, key)
		go func(c chan string, v string) { c <- v }(ch, val)
	}
	cmd.Client.WriteInt(len(entry.list))
}

func HandleLRANGE(server *Redis, cmd Command) {
	server.mu.Lock()
	defer server.mu.Unlock()
	key := cmd.Args[1]
	entry, ok := server.dict[key]
	if !ok {
		cmd.Client.WriteEmpty()
		return
	}
	if len(cmd.Args) < 4 {
		cmd.Client.WriteErr("Not enough arguments for LRANGE")
		return
	}

	start, _ := strconv.Atoi(cmd.Args[2])
	end, _ := strconv.Atoi(cmd.Args[3])
	n := len(entry.list)
	if start < 0 {
		start = n + start
		if start < 0 {
			start = 0
		}
	}
	if end < 0 {
		end = n + end
		if end < 0 {
			end = 0
		}
	}
	if (start > end) || (start > n) {
		cmd.Client.WriteEmpty()
		return
	}
	if end >= n {
		end = n - 1
	}
	lslice := entry.list[start : end+1]
	cmd.Client.WriteList(lslice)
}

func HandleLLEN(server *Redis, cmd Command) {
	server.mu.Lock()
	defer server.mu.Unlock()
	if len(cmd.Args) < 2 {
		cmd.Client.WriteErr("Key required for the list item")
		return
	}
	key := cmd.Args[1]
	entry, ok := server.dict[key]
	if !ok {
		cmd.Client.WriteInt(0)
	} else {
		cmd.Client.WriteInt(len(entry.list))
	}
}

func HandleLPOP(server *Redis, cmd Command) {
	server.mu.Lock()
	defer server.mu.Unlock()
	if len(cmd.Args) < 2 {
		cmd.Client.WriteErr("Key required for the list item")
		return
	}
	key := cmd.Args[1]
	entry, ok := server.dict[key]
	if !ok || len(entry.list) == 0 {
		cmd.Client.WriteNil()
		return
	}
	n := len(entry.list)
	if len(cmd.Args) >= 3 {
		idx, _ := strconv.Atoi(cmd.Args[2])
		if idx > n {
			idx = n
		}
		front := entry.list[:idx]
		entry.list = entry.list[idx:]
		server.dict[key] = entry
		cmd.Client.WriteList(front)
	} else {
		front := entry.list[0]
		entry.list = entry.list[1:]
		server.dict[key] = entry
		cmd.Client.WriteBulkString(front)
	}
}

func HandleBLPOP(server *Redis, cmd Command) {
	if len(cmd.Args) < 3 {
		cmd.Client.WriteErr("Invalid usage: BLPOP key MX_TIME")
		return
	}
	key := cmd.Args[1]
	timeoutSec, _ := strconv.ParseFloat(cmd.Args[2], 64)
	timeout := time.Duration(timeoutSec*1000) * time.Millisecond
	server.mu.Lock()
	entry, ok := server.dict[key]
	if ok && len(entry.list) > 0 {
		val := entry.list[0]
		entry.list = entry.list[1:]
		server.dict[key] = entry
		server.mu.Unlock()
		cmd.Client.WriteList([]string{key, val})
		return
	}
	ch := make(chan string)
	server.waiters[key] = ch
	server.mu.Unlock()
	if timeout == 0.0 {
		val := <-ch
		cmd.Client.WriteList([]string{key, val})
	}
	select {
	case val := <-ch:
		cmd.Client.WriteList([]string{key, val})
	case <-time.After(timeout):
		server.mu.Lock()
		delete(server.waiters, key)
		server.mu.Unlock()
		cmd.Client.WriteNil()
	}
}
