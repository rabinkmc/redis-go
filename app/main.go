package main

import (
	"bufio"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
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
	server.dict[key] = entry
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
	server.readRDB()
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

func HandleRPUSH(server *Redis, cmd Command) {
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
		entry := server.dict[key]
		val := entry.list[0]
		entry.list = entry.list[1:]
		server.dict[key] = entry
		delete(server.waiters, key)
		go func() {
			ch <- val
		}()
	}
	cmd.Client.WriteInt(n)
}

func HandleLPUSH(server *Redis, cmd Command) {
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
	n := len(entry.list)

	if ch, exists := server.waiters[key]; exists {
		entry := server.dict[key]
		val := values[len(values)-1]
		entry.list = entry.list[1:]
		server.dict[key] = entry
		delete(server.waiters, key)
		go func() {
			ch <- val
		}()
	}
	cmd.Client.WriteInt(n)
}

func HandleLRANGE(server *Redis, cmd Command) {
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
	entry, ok := server.dict[key]
	if ok && len(entry.list) > 0 {
		val := entry.list[0]
		entry.list = entry.list[1:]
		server.dict[key] = entry
		cmd.Client.WriteList([]string{key, val})
	}
	ch := make(chan string)
	server.waiters[key] = ch
	if timeout == 0.0 {
		val := <-ch
		cmd.Client.WriteList([]string{key, val})
	}
	select {
	case val := <-ch:
		cmd.Client.WriteList([]string{key, val})
	case <-time.After(timeout):
		delete(server.waiters, key)
		cmd.Client.WriteNil()
	}
}

func HandleTYPE(server *Redis, cmd Command) {
	key := cmd.Args[1]
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

func (entry *Entry) NewStream(stream_id string, items []string) (*Stream, string) {
	if stream_id == "0-0" {
		return nil, simple_err("The ID specified in XADD must be greater than 0-0")
	}
	n := len(entry.streams)
	if stream_id == "*" {
		unix_time := time.Now().UnixMilli()
		_, ok := entry.streamMS[unix_time]
		if !ok {
			stream_id = fmt.Sprintf("%d-%d", unix_time, 0)
		} else {
			prev_stream := entry.streams[n-1]
			stream_id = fmt.Sprintf("%d-%d", unix_time, prev_stream.seq+1)
		}
	}
	if len(stream_id) >= 2 && stream_id[len(stream_id)-2:] == "-*" {
		part1, _ := strconv.ParseInt(stream_id[:strings.Index(stream_id, "-")], 10, 64)
		if n > 0 && part1 < entry.streams[len(entry.streams)-1].time {
			return nil, simple_err("The ID specified in XADD is equal or smaller than the target stream top item")
		}
		_, ok := entry.streamMS[part1]
		part2 := int64(0)
		if !ok {
			if part1 == 0 {
				part2 = 1
			}
		} else {
			prev_stream := entry.streams[n-1]
			part2 = prev_stream.seq + 1
		}
		stream_id = fmt.Sprintf("%d-%d", part1, part2)
	}
	parts := strings.Split(stream_id, "-")
	if len(parts) < 2 {
		return nil, simple_err("Invalid id: required as (1-2)")
	}
	time, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, err.Error()
	}
	seq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, err.Error()
	}

	stream := &Stream{
		id:    stream_id,
		items: items,
		time:  time,
		seq:   seq,
	}
	if n == 0 {
		return stream, ""
	}
	prev_stream := entry.streams[n-1]
	if stream.time > prev_stream.time {
		return stream, ""
	}
	case1 := stream.time < prev_stream.time
	case2 := (stream.time == prev_stream.time) && (stream.seq <= prev_stream.seq)
	if case1 || case2 {
		return nil, simple_err("The ID specified in XADD is equal or smaller than the target stream top item")
	}
	return stream, ""

}

func HandleXADD(server *Redis, cmd Command) {
	if len(cmd.Args) < 4 {
		cmd.Client.WriteErr("Invalid usage: XADD stream_key stream_id field val [field val...]")
	}
	key := cmd.Args[1]
	id := cmd.Args[2]
	entry, _ := server.dict[key]
	if len(entry.streams) == 0 {
		entry.streamMS = make(map[int64]int)
	}
	stream, err := entry.NewStream(id, cmd.Args[3:])
	if err != "" {
		cmd.Client.WriteErr(err)
		return
	}
	entry.streams = append(entry.streams, *stream)
	_, ok := entry.streamMS[stream.time]
	if !ok {
		entry.streamMS[stream.time] = len(entry.streams) - 1
	}
	server.dict[key] = entry

	if ch, exists := server.stream_waiters[key+","+"$"]; exists {
		arr := "*1\r\n" + "*2\r\n" + resp_bulk_string(stream.id) + encode_list(stream.items)
		xread_val := "*1\r\n" + "*2\r\n" + resp_bulk_string(key) + arr
		delete(server.waiters, key)
		go func() {
			ch <- xread_val
		}()
		cmd.Client.WriteBulkString(stream.id)
		return
	}

	// brute force solution
	for stream_key, ch := range server.stream_waiters {
		parts := strings.Split(stream_key, ",")
		if parts[0] != key {
			continue
		}
		if parts[1] >= id {
			continue
		}
		arr := "*1\r\n" + "*2\r\n" + resp_bulk_string(stream.id) + encode_list(stream.items)
		xread_val := "*1\r\n" + "*2\r\n" + resp_bulk_string(key) + arr
		delete(server.waiters, key)
		go func(s string) {
			ch <- s
		}(xread_val)
	}

	cmd.Client.WriteBulkString(stream.id)
}

func HandleXRANGE(server *Redis, cmd Command) {
	if len(cmd.Args) < 4 {
		cmd.Client.WriteErr("Invalid usage: XRANGE key start_seq end_seq")
		return
	}
	key := cmd.Args[1]
	entry, _ := server.dict[key]
	start_index := 0
	end_index := len(entry.streams) - 1
	if cmd.Args[2] != "-" {
		start_index = bsearch_gte(entry.streams, cmd.Args[2])
	}
	if cmd.Args[3] != "+" {
		end_index = bsearch_lte(entry.streams, cmd.Args[3])
	}
	if start_index == -1 || end_index == -1 {
		cmd.Client.WriteNil()
	}
	stream_count := 0
	var b strings.Builder
	for i := start_index; i <= end_index; i++ {
		stream := entry.streams[i]
		curr_str := "*2\r\n" + resp_bulk_string(stream.id) + encode_list(stream.items)
		stream_count += 1
		b.WriteString(curr_str)
	}
	resp := fmt.Sprintf("*%d\r\n%s", stream_count, b.String())
	cmd.Client.WriteString(resp)
}

func HandleXREAD(server *Redis, cmd Command) {
	// assuming the second command is STREAMS
	// assert args[1] == STREAMS
	// single case

	//multiple case
	// suppose there are 10 arguments
	// args[0] == XREAD
	// args[1] == XSTREAM
	// now total arg pair == 4
	// arg[2], arg[3], arg[4], arg[5] are keys
	// arg[6], arg[7], arg[8], arg[9] are values
	// so we have *4\r\n items
	// total loop = total_pairs == (len(args) - 2) / 2
	// if block is not specified this works fine
	// however, if block exists
	// we have two cases
	// one if data exists we return
	// else we create a channel, put channel in our waiter map, we wait for the sender to send the data to that channel
	// once we have the data we proceed as before
	args := cmd.Args[2:]
	if len(args) < 2 || len(args)%2 == 1 {
		log.Fatalf("Invalid arguments")
	}
	n_keys := len(args) / 2
	var b strings.Builder
	b.WriteString(fmt.Sprintf("*%d\r\n", n_keys))
	for i := 0; i < n_keys; i++ {
		stream_key := args[i]
		stream_id := args[i+n_keys]
		resp := HandleXREADSINGLE(server, stream_key, stream_id)
		b.WriteString(resp)
	}
	cmd.Client.WriteString(b.String())
}

func (entry *Entry) stream_exists(id string) bool {
	return bsearch_gt(entry.streams, id) != -1
}

func HandleXREADBLOCK(server *Redis, cmd Command) {
	// args[0] == time
	// args[1] == STREAMS
	key := cmd.Args[3]
	id := cmd.Args[4]
	entry, ok := server.dict[key]
	if id != "$" && ok && entry.stream_exists(id) {
		resp := HandleXREADSINGLE(server, key, id)
		cmd.Client.WriteString(resp)
	}
	mtime, _ := strconv.ParseFloat(cmd.Args[1], 64)
	timeout := time.Duration(mtime) * time.Millisecond
	ch := make(chan string)
	waiting_key := key + "," + id
	server.stream_waiters[waiting_key] = ch
	if timeout == 0.0 {
		val := <-ch
		cmd.Client.WriteString(val)
	}
	select {
	case val := <-ch:
		cmd.Client.WriteString(val)
	case <-time.After(timeout):
		delete(server.waiters, id)
		cmd.Client.WriteNil()
	}
}

func HandleXREADSINGLE(server *Redis, key, stream_id string) string {
	// this function shouldn't be called if there is no data
	entry, _ := server.dict[key]
	start_idx := bsearch_gt(entry.streams, stream_id)
	end_idx := len(entry.streams) - 1
	stream_count := 0
	result := ""
	// Handle for start_idx
	// i should filp the operations
	// entry.StreamMS should hold the MS_Key and give me the starting address
	for i := start_idx; i <= end_idx; i++ {
		curr_str := "*2\r\n" + resp_bulk_string(entry.streams[i].id) + encode_list(entry.streams[i].items)
		result = result + curr_str
		stream_count += 1
	}
	// Handle for end_idx
	value := fmt.Sprintf("*%d\r\n%s", stream_count, result)
	final := "*2\r\n" + resp_bulk_string(key) + value
	return final
}

func HandleINCR(server *Redis, cmd Command) {
	key := cmd.Args[1]
	entry, ok := server.dict[key]
	if !ok {
		entry = Entry{val: "0"}
		server.dict[key] = entry
	}
	int_val, err := strconv.Atoi(entry.val)
	if err != nil {
		cmd.Client.WriteErr("value is not an integer or out of range")
	}
	entry.val = strconv.Itoa(int_val + 1)
	server.dict[key] = entry
	cmd.Client.WriteInt(int_val + 1)
}

func HandleMULTI(server *Redis, cmd Command) {
	client := cmd.Client
	client.queue = true
	cmd.Client.WriteStatus("OK")
}

func HandleDISCARD(server *Redis, cmd Command) {
	client := cmd.Client
	if !client.queue {
		cmd.Client.WriteErr("DISCARD without MULTI")
	}
	client.queue = false
	client.commands = [][]string{}
	cmd.Client.WriteStatus("OK")
}

func HandleEXEC(server *Redis, cmd Command) {
	client := cmd.Client
	if !client.queue {
		cmd.Client.WriteErr("EXEC without MULTI")
	}
	client.queue = false
	cmd.Client.WriteString(fmt.Sprintf("*%d\r\n", len(client.commands)))
	for _, cmds := range client.commands {
		command := Command{Client: client, Args: cmds}
		Execute(server, command)
	}
	client.commands = [][]string{}
}

func HandleINFO(server *Redis, cmd Command) {
	conn := cmd.Client.conn
	info_key := cmd.Args[1]
	section_info, exists := server.info[info_key]
	if !exists {
		conn.Write([]byte(simple_err("Key doesn't exist")))
	}
	var b strings.Builder
	for key, val := range section_info {
		st := fmt.Sprintf("%s:%s\r\n", key, val)
		b.WriteString(st)
	}
	res := b.String()
	cmd.Client.WriteBulkString(res[:len(res)-2])
}

func HandleREPLCONF(server *Redis, cmd Command) {
	slave_exists := func(server *Redis, conn net.Conn) bool {
		for _, c := range server.slaves {
			if c == conn {
				return true
			}
		}
		return false
	}
	cmdName := strings.ToUpper(cmd.Args[1])
	client := cmd.Client
	conn := client.conn
	if cmdName == "LISTENING-PORT" {
		if slave_exists(server, conn) {
			conn.Write([]byte("+OK\r\n"))
			return
		}
		// otherwise add
		server.slaves = append(server.slaves, conn)
		cmd.Client.WriteStatus("OK")
		return
	}
	if cmdName == "ACK" {
		offset, err := strconv.Atoi(cmd.Args[2])
		if err != nil {
			log.Fatalf("Error parsing integer %v", err)
		}
		server.ack_slaves[conn] = offset
		for _, w := range server.replica_waiters {
			if server.slave_sync_count(w.offset) >= w.required {
				select {
				case w.ch <- struct{}{}:
				default:
				}
				server.removeWaiter(w)
			}
		}
		return
	}
	cmd.Client.WriteStatus("OK")
}

func HandlePSYNC(server *Redis, cmd Command) {
	args := cmd.Args[1:]
	if args[1] == "?" {
		args[1] = server.id
	}
	if args[2] == "-1" {
		args[2] = "0"
	}
	resp := fmt.Sprintf("FULLRESYNC %s %s", args[1], args[2])
	cmd.Client.WriteStatus(resp)
	server.writeFile(cmd.Client.conn)
}

func HandleWAIT(server *Redis, cmd Command) {
	args := cmd.Args[1:]
	conn := cmd.Client.conn
	replicas, _ := strconv.Atoi(args[0])
	wait_offset := server.master_offset
	count := server.slave_sync_count(wait_offset)
	if count >= replicas || wait_offset == 0 {
		conn.Write([]byte(resp_int(count)))
		return
	}
	request := []byte(encode_list([]string{"REPLCONF", "GETACK", "*"}))
	server.master_offset += len(request)
	for _, slave_conn := range server.slaves {
		_, err := slave_conn.Write(request)
		if err != nil {
			log.Printf("Error writing to the connection: %v", err)
		}
	}

	w := &WaitRequest{
		required: replicas,
		offset:   wait_offset,
		ch:       make(chan struct{}),
	}
	server.replica_waiters = append(server.replica_waiters, w)

	mtime, _ := strconv.ParseFloat(args[1], 64)
	timeout := time.Duration(mtime) * time.Millisecond
	select {
	case <-w.ch:
		conn.Write([]byte(resp_int(replicas)))
	case <-time.After(timeout):
		count = server.slave_sync_count(w.offset)
		conn.Write([]byte(resp_int(count)))
		server.removeWaiter(w)
	}
}

func Execute(server *Redis, cmd Command) {
	cmdName := strings.ToUpper(cmd.Args[0])
	switch cmdName {
	case "ECHO":
		HandleECHO(server, cmd)
	case "PING":
		HandlePING(server, cmd)
	case "SET":
		HandleSET(server, cmd)
	case "GET":
		HandleGET(server, cmd)
	case "RPUSH":
		HandleRPUSH(server, cmd)
	case "LPUSH":
		HandleLPUSH(server, cmd)
	case "LRANGE":
		HandleLRANGE(server, cmd)
	case "LLEN":
		HandleLLEN(server, cmd)
	case "LPOP":
		HandleLPOP(server, cmd)
	case "BLPOP":
		HandleBLPOP(server, cmd)
	case "TYPE":
		HandleTYPE(server, cmd)
	case "XADD":
		HandleXADD(server, cmd)
	case "XRANGE":
		HandleXRANGE(server, cmd)
	case "XREAD":
		if strings.ToUpper(cmd.Args[1]) == "BLOCK" {
			HandleXREADBLOCK(server, cmd)
		} else {
			HandleXREAD(server, cmd)
		}
	case "INCR":
		HandleINCR(server, cmd)
	case "MULTI":
		HandleMULTI(server, cmd)
	case "DISCARD":
		HandleDISCARD(server, cmd)
	case "EXEC":
		HandleEXEC(server, cmd)
	case "INFO":
		HandleINFO(server, cmd)
	case "CONFIG":
		HandleCONFIG(server, cmd)
	case "KEYS":
		HandleKEYS(server, cmd)
	default:
		cmd.Client.WriteErr("Unrecognized command")
	}
}

func HandleCONFIG(server *Redis, cmd Command) {
	args := cmd.Args[1:]
	key := args[1]
	switch cmd.Args[0] {
	case "GET":
		if key == "dir" {
			cmd.Client.WriteList([]string{"dir", server.rdb_dir})
		}
		if key == "dbfilename" {
			cmd.Client.WriteList([]string{"dir", server.dbfilename})
		}
	default:
		cmd.Client.WriteNil()
	}
	cmd.Client.WriteNil()

}
func (server *Redis) writeFile(conn net.Conn) {
	emptyRDB := "524544495330303131fa0972656469732d76657205372e322e30" +
		"fa0a72656469732d62697473c040fa056374696d65c26d08bc65" +
		"fa08757365642d6d656dc2b0c41000fa08616f662d62617365c0" +
		"00fff06e3bfec0ff5aa2"
	bytes, err := hex.DecodeString(emptyRDB)
	if err != nil {
		log.Fatalf("Error decoding hex string: %v", err.Error())
	}

	data := string(bytes)
	resp := fmt.Sprintf("$%d\r\n%s", len(data), data)
	_, err = conn.Write([]byte(resp))

	if err != nil {
		log.Fatalf("Error writing to connection: %v", err.Error())
		return
	}
}

func (server *Redis) HandleReplConnection(conn net.Conn, handshake chan string) {
	defer conn.Close()
	client := &Client{}
	offset := 0
	reader := bufio.NewReader(conn)
	for {
		ch, err := reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				log.Println("Master closed the connection")
			} else {
				log.Printf("Connection error: %v\n", err)
			}
			return
		}

		switch ch {
		case '+':
			line, err := reader.ReadString('\n')
			if err != nil {
				log.Println("Error reading the response")
				return
			}
			line = strings.TrimSpace(line)
			if line == "PONG" {
				handshake <- "PONG"

			} else if line == "OK" {
				handshake <- "OK"
			} else if strings.HasPrefix(line, "FULLRESYNC") {
				continue
			}
		case '$':
			// reading rdb file
			line, err := reader.ReadString('\n')
			line = strings.TrimSuffix(line, "\r\n")
			rdb_size, err := strconv.Atoi(line)
			if err != nil {
				log.Printf("Invalid RDB size: %v\n", err)
				continue
			}
			buf := make([]byte, rdb_size)
			_, err = io.ReadFull(reader, buf)
			if err != nil {
				log.Printf("Failed to read full RDB: %v\n", err)
				return
			}
		case '*':
			// propagation commands
			line, err := reader.ReadString('\n')
			if err != nil {
				log.Printf("Error reading array size: %v\n", err)
				return
			}
			line = strings.TrimSuffix(line, "\r\n")
			arr_size, err := strconv.Atoi(line[:])
			if err != nil {
				log.Printf("Protocol error: invalid array size %s\n", line)
				return
			}
			// resp arr
			args, err := parse_resp_arr(reader, arr_size)
			if err != nil {
				log.Printf("Error parsing RESP array: %v\n", err)
				return
			}
			cmdRaw := encode_list(args)
			curr_length := len(cmdRaw)

			cmd := strings.ToUpper(args[0])
			if cmd == "REPLCONF" && len(args) > 1 && strings.ToUpper(args[1]) == "GETACK" {
				resp := encode_list([]string{"REPLCONF", "ACK", strconv.Itoa(offset)})
				conn.Write([]byte(resp))
			} else if cmd != "DISCARD" && cmd != "EXEC" && client.queue {
				client.commands = append(client.commands, args)
			} else {
				command := Command{Client: client, Args: args}
				Execute(server, command)
			}
			offset = offset + curr_length
		}
	}
}

func (server *Redis) HandleConnection(conn net.Conn) {
	client := NewClient(conn)
	defer client.Close()
	reader := NewRespReader(conn)
	nopass_checked := false
	var nopass = false
	for {
		args, err := reader.ReadCommand()
		if err != nil {
			log.Printf("error reading from the connection: %v", err)
			continue
		}
		if client.user == nil {
			client.user = server.users["default"]
		}

		if !nopass_checked {
			nopass = client.user.nopass()
			nopass_checked = true
		}

		cmd := strings.ToUpper(args[0])
		if !nopass && !client.auth && cmd != "AUTH" {
			resp := "-NOAUTH Authentication required\r\n"
			client.conn.Write([]byte(resp))
			continue
		}

		command := Command{
			Client: client,
			Args:   args,
		}

		if in_subscription_mode(command) {
			HandleSubscription(server, command)
			continue
		}
		if is_acl_cmd(command) {
			HandleACL(server, command)
			continue
		}
		if is_geo_cmd(command) {
			HandleGeo(server, command)
			continue
		}
		if is_set_cmd(command) {
			HandleZset(server, command)
			continue
		}
		if cmd == "WAIT" {
			HandleWAIT(server, command)
			continue
		}
		if cmd == "REPLCONF" {
			HandleREPLCONF(server, command)
			continue
		} else if cmd == "PSYNC" {
			HandlePSYNC(server, command)
			continue
		} else if cmd != "DISCARD" && cmd != "EXEC" && client.queue {
			client.commands = append(client.commands, args)
			command.Client.WriteStatus("QUEUED")
		} else {
			Execute(server, command)
		}
		server.write_slaves(cmd, []byte(encode_list(args)))
	}
}

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
