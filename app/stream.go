package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
)

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
	server.mu.Lock()
	entry, ok := server.dict[key]
	if !ok {
		entry = Entry{streamMS: make(map[int64]int)}
	}
	stream, errStr := entry.NewStream(id, cmd.Args[3:])
	if errStr != "" {
		server.mu.Unlock()
		cmd.Client.WriteErr(errStr)
		return
	}

	entry.streams = append(entry.streams, *stream)
	if _, exists := entry.streamMS[stream.time]; !exists {
		entry.streamMS[stream.time] = len(entry.streams) - 1
	}
	server.dict[key] = entry

	if ch, exists := server.stream_waiters[key+","+"$"]; exists {
		arr := "*1\r\n" + "*2\r\n" + resp_bulk_string(stream.id) + encode_list(stream.items)
		xread_val := "*1\r\n" + "*2\r\n" + resp_bulk_string(key) + arr
		delete(server.waiters, key)
		go func(c chan string, v string) {
			c <- v
		}(ch, xread_val)
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
