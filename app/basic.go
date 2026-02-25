package main

import (
	"strconv"
	"time"
)

func HandlePING(server *Redis, cmd Command) {
	cmd.Client.WriteStatus("PONG")
}

func HandleECHO(server *Redis, cmd Command) {
	cmd.Client.WriteBulkString(cmd.Args[1])
}

func HandleSET(server *Redis, cmd Command) {
	if len(cmd.Args) < 3 {
		cmd.Client.WriteErr(
			"Not enough args for SET: SET key val [EX|PX] [time]",
		)
		return
	}
	key := cmd.Args[1]
	value := cmd.Args[2]
	entry := Entry{val: value}
	if len(cmd.Args) >= 5 {
		ex_time := time.Now()
		if cmd.Args[3] == "PX" {
			ms, _ := strconv.Atoi(cmd.Args[4])
			ex_time = ex_time.Add(time.Duration(ms) * time.Millisecond)
		} else if cmd.Args[3] == "EX" {
			sec, _ := strconv.Atoi(cmd.Args[4])
			ex_time = ex_time.Add(time.Duration(sec) * time.Second)
		}
		entry.time = &ex_time
	}
	server.mu.Lock()
	server.dict[key] = entry
	server.mu.Unlock()
	cmd.Client.WriteStatus("OK")
}

func HandleGET(server *Redis, cmd Command) {
	if len(cmd.Args) < 2 {
		cmd.Client.WriteErr(
			"Get requires a key",
		)
		return
	}
	key := cmd.Args[1]
	server.mu.Lock()
	defer server.mu.Unlock()
	entry, ok := server.dict[key]
	if !ok {
		cmd.Client.WriteNil()
		return
	}
	if entry.time != nil && entry.time.Before(time.Now()) {
		delete(server.dict, key)
		cmd.Client.WriteNil()
		return
	}
	cmd.Client.WriteBulkString(entry.val)
}

func HandleINCR(server *Redis, cmd Command) {
	key := cmd.Args[1]
	server.mu.Lock()
	defer server.mu.Unlock()
	entry, ok := server.dict[key]
	if !ok {
		entry = Entry{val: "0"}
		server.dict[key] = entry
	}
	int_val, err := strconv.Atoi(entry.val)
	if err != nil {
		cmd.Client.WriteErr("value is not an integer or out of range")
		return
	}
	entry.val = strconv.Itoa(int_val + 1)
	server.dict[key] = entry
	cmd.Client.WriteInt(int_val + 1)
}

func HandleTYPE(server *Redis, cmd Command) {
	key := cmd.Args[1]
	server.mu.RLock()
	defer server.mu.RUnlock()
	entry, ok := server.dict[key]
	resp := "none"
	if !ok {
		cmd.Client.WriteStatus(resp)
		return
	}
	if entry.val != "" {
		resp = "string"
	} else if len(entry.streams) > 0 {
		resp = "stream"
	} else if len(entry.list) > 0 {
		resp = "list"
	}
	cmd.Client.WriteStatus(resp)
}

func HandleKEYS(server *Redis, cmd Command) {
	server.mu.RLock()
	if server.rdb_read_status {
		res := []string{}
		for key := range server.dict {
			res = append(res, key)
		}
		server.mu.RUnlock()
		cmd.Client.WriteList(res)
		return
	}
	server.mu.RUnlock()

	server.mu.Lock()
	defer server.mu.Unlock()

	if !server.rdb_read_status {
		server.readRDB()
		server.rdb_read_status = true
	}

	res := []string{}
	for key := range server.dict {
		res = append(res, key)
	}
	cmd.Client.WriteList(res)
	return
}
