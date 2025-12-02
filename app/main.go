package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Stream struct {
	id    string
	items []string // key, val pair
}

type Entry struct {
	val      string
	list     []string
	streams  map[int64][]Stream
	streamMS []int64
	time     *time.Time
}

type Redis struct {
	dict    map[string]Entry
	mu      sync.Mutex
	waiters map[string]chan string
}

func NewRedis() *Redis {
	return &Redis{dict: make(map[string]Entry), waiters: make(map[string]chan string)}
}

func encode(str string) string {
	result := fmt.Sprintf("$%d\r\n%s\r\n", len(str), str)
	return result
}

func simple_err(str string) string {
	return fmt.Sprintf("-ERR %s\r\n", str)
}
func encode_list(strs []string) string {
	if len(strs) == 0 {
		return "*0\r\n"
	}
	result := fmt.Sprintf("*%d\r\n", len(strs))
	for _, str := range strs {
		result += fmt.Sprintf("$%d\r\n%s\r\n", len(str), str)
	}
	return result
}

func (server *Redis) handlePING() string {
	return "+PONG\r\n"
}

func (server *Redis) handleECHO(arg string) string {
	return encode(arg)
}

func (server *Redis) handleSET(args []string) string {
	key := args[0]
	value := args[1]
	entry := Entry{val: value}
	if len(args) >= 4 {
		ex_time := time.Now()
		if args[2] == "PX" {
			ms, _ := strconv.Atoi(args[3])
			ex_time = ex_time.Add(time.Duration(ms) * time.Millisecond)
		} else if args[2] == "EX" {
			sec, _ := strconv.Atoi(args[3])
			ex_time = ex_time.Add(time.Duration(sec) * time.Second)
		}
		entry.time = &ex_time
	}
	server.dict[key] = entry
	return "+OK\r\n"
}

func (server *Redis) handleGET(key string) string {
	entry, ok := server.dict[key]
	fmt.Printf("time: %v", entry.time)
	if !ok {
		return "$-1\r\n"
	}
	if entry.time != nil && entry.time.Before(time.Now()) {
		delete(server.dict, key)
		return "$-1\r\n"
	}

	return encode(entry.val)
}

func (server *Redis) handleRPUSH(args []string) string {
	server.mu.Lock()
	defer server.mu.Unlock()
	key := args[0]
	values := args[1:]
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
	resp := fmt.Sprintf(":%d\r\n", n)
	return resp
}

func (server *Redis) handleLPUSH(args []string) string {
	server.mu.Lock()
	defer server.mu.Unlock()
	key := args[0]
	values := args[1:]
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
	resp := fmt.Sprintf(":%d\r\n", n)
	return resp
}

func (server *Redis) handleLRANGE(args []string) string {
	key := args[0]
	empty_arr := "*0\r\n"
	entry, ok := server.dict[key]
	// key doesn't exist
	if !ok {
		return empty_arr
	}

	start, _ := strconv.Atoi(args[1])
	end, _ := strconv.Atoi(args[2])
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
		return empty_arr
	}

	// fix the upper bounds
	if end >= n {
		end = n - 1
	}
	lslice := entry.list[start : end+1]

	resp := encode_list(lslice)
	return resp
}

func (server *Redis) handleLLEN(key string) string {
	zero := ":0\r\n"
	entry, ok := server.dict[key]
	if !ok {
		return zero
	}
	var n int = len(entry.list)
	resp := fmt.Sprintf(":%d\r\n", n)
	return resp
}

func (server *Redis) handleLPOP(args []string) string {
	key := args[0]
	entry, ok := server.dict[key]
	if !ok || len(entry.list) == 0 {
		return "$-1\r\n"
	}
	n := len(entry.list)
	if len(args) >= 2 {
		idx, _ := strconv.Atoi(args[1])
		if idx > n {
			idx = n
		}
		front := entry.list[:idx]
		entry.list = entry.list[idx:]
		server.dict[key] = entry
		return encode_list(front)
	} else {
		front := entry.list[0]
		entry.list = entry.list[1:]
		server.dict[key] = entry
		return encode(front)
	}
}

func (server *Redis) handleBLPOP(args []string) string {
	server.mu.Lock()
	key := args[0]
	timeoutSec, _ := strconv.ParseFloat(args[1], 64)
	timeout := time.Duration(timeoutSec*1000) * time.Millisecond
	entry, ok := server.dict[key]
	if ok && len(entry.list) > 0 {
		val := entry.list[0]
		entry.list = entry.list[1:]
		server.dict[key] = entry
		server.mu.Unlock()
		return encode_list([]string{key, val})
	}
	ch := make(chan string)
	server.waiters[key] = ch
	server.mu.Unlock()
	if timeout == 0.0 {
		val := <-ch
		return encode_list([]string{key, val})
	}
	select {
	case val := <-ch:
		return encode_list([]string{key, val})
	case <-time.After(timeout):
		server.mu.Lock()
		delete(server.waiters, key)
		server.mu.Unlock()
		return "*-1\r\n"
	}

}
func (server *Redis) handleTYPE(args []string) string {
	key := args[0]
	entry, ok := server.dict[key]
	resp := "none"
	if !ok {
		return fmt.Sprintf("+%s\r\n", resp)
	}
	if entry.val != "" {
		resp = "string"
	} else if len(entry.streams) > 0 {
		resp = "stream"
	} else if len(entry.list) > 0 {
		resp = "list"
	}
	return fmt.Sprintf("+%s\r\n", resp)
}

// ms := time.Now().UnixMilli()
func (entry *Entry) validate_id(millitime int64, id string) (string, string) {
	streams := entry.streams[millitime]
	var t2, s2 int64
	var err error
	new_id := id
	if new_id == "*" {
		unix_time := time.Now().UnixMilli()
		streams, ok := entry.streams[unix_time]
		if !ok {
			return fmt.Sprintf("%d-%d", unix_time, 0), ""
		}
		seq, _ := strconv.ParseInt(strings.Split(streams[len(streams)-1].id, "-")[1], 10, 64)
		return fmt.Sprintf("%d-%d", unix_time, seq+1), ""
	}
	parts := strings.Split(new_id, "-")
	t2, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return new_id, fmt.Sprintf("Failed to parse time for new ID: %s", parts)
	}
	if parts[1] != "*" {
		s2, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return new_id, fmt.Sprintf("Failed to parse sequence for new ID: %s", parts)
		}
	} else {
		if len(streams) == 0 {
			if t2 == 0 {
				s2 = 1
			} else {
				s2 = 0
			}
		} else {
			prev_id := streams[len(streams)-1].id
			prev_id_parts := strings.Split(prev_id, "-")
			prev_seq, _ := strconv.ParseInt(prev_id_parts[1], 10, 64)
			s2 = prev_seq + 1
		}
	}
	new_id = fmt.Sprintf("%d-%d", t2, s2)
	if t2 <= 0 && s2 <= 0 {
		return new_id, fmt.Sprintf("The ID specified in XADD must be greater than 0-0")
	}

	top := len(entry.streamMS) - 1
	if top == -1 {
		return new_id, ""
	}

	t1 := entry.streamMS[top]
	error_str := fmt.Sprintf("The ID specified in XADD is equal or smaller than the target stream top item")
	if t2 < t1 {
		return new_id, error_str
	}
	var s1 int64 = 0

	if len(streams) > 0 {
		prev_id := streams[len(streams)-1].id
		prev_id_parts := strings.Split(prev_id, "-")
		s1, _ = strconv.ParseInt(prev_id_parts[1], 10, 64)
	}

	if (t1 == t2) && (s2 <= s1) {
		return new_id, error_str
	}
	return new_id, ""
}

func bsearch(arr []int64, target int64) int {
	left, right := 0, len(arr)-1
	for left <= right {
		m := left + (right-left)/2
		if arr[m] == target {
			return m
		} else if arr[m] > target {
			right = m - 1
		} else {
			left = m + 1
		}
	}
	return -1
}

func bsearch_seq(arr []Stream, target int64) int {
	left, right := 0, len(arr)-1
	for left <= right {
		m := left + (right-left)/2
		id := arr[m].id
		parts := strings.Split(id, "-")
		seq, _ := strconv.ParseInt(parts[1], 10, 64)
		log.Printf("sequence printing %d\n", seq)
		if seq == target {
			return m
		} else if seq > target {
			right = m - 1
		} else {
			left = m + 1
		}
	}
	return -1
}

func (server *Redis) handleXADD(args []string) string {
	stream_key := args[0]
	id := args[1]
	n := len(args)
	i := 2
	entry, _ := server.dict[stream_key]
	if entry.streams == nil {
		entry.streams = make(map[int64][]Stream)
	}
	parts := strings.Split(id, "-")

	millitime, _ := strconv.ParseInt(parts[0], 10, 64)

	id, error_str := entry.validate_id(millitime, id)
	if error_str != "" {
		return simple_err(error_str)
	}
	items := []string{}
	for i < n {
		key := args[i]
		val := args[i+1]
		items = append(items, key)
		items = append(items, val)
		i = i + 2
	}
	entry.streams[millitime] = append(entry.streams[millitime], Stream{id: id, items: items})

	ms_size := len(entry.streamMS)
	if ms_size == 0 || millitime > entry.streamMS[ms_size-1] {
		entry.streamMS = append(entry.streamMS, millitime)
	}

	server.dict[stream_key] = entry

	return encode(id)
}

func (server *Redis) handleXRANGE(args []string) string {
	key := args[0]
	entry, _ := server.dict[key]
	start_parts := strings.Split(args[1], "-")
	end_parts := strings.Split(args[2], "-")
	start, _ := strconv.ParseInt(start_parts[0], 10, 64)
	start_seq := int64(0)
	end_seq := int64(0)
	if len(start_parts) > 1 {
		start_seq, _ = strconv.ParseInt(start_parts[1], 10, 64)
	}
	if len(end_parts) > 1 {
		end_seq, _ = strconv.ParseInt(end_parts[1], 10, 64)
	}
	end, _ := strconv.ParseInt(end_parts[0], 10, 64)

	start_idx := bsearch(entry.streamMS, start)
	end_idx := bsearch(entry.streamMS, end)

	stream_count := 0
	result := ""

	// handle for start_idx
	for i := start_idx; i <= end_idx; i++ {
		ms_key := entry.streamMS[i]
		ms_streams := entry.streams[ms_key]
		sseq_idx := 0
		if i == start_idx && start_seq > 0 {
			sseq_idx = bsearch_seq(ms_streams, start_seq)
		}
		eseq_idx := len(ms_streams) - 1
		if i == end_idx && end_seq > 0 {
			if end_seq != 0 {
				eseq_idx = bsearch_seq(ms_streams, end_seq)
			}
		}
		for j := sseq_idx; j <= eseq_idx; j++ {
			curr := []string{}
			stream := ms_streams[j]
			curr = append(curr, stream.items...)
			curr_str := "*2\r\n" + encode(stream.id) + encode_list(curr)
			stream_count += 1
			result = result + curr_str
		}
	}
	// handle for end_idx
	return fmt.Sprintf("*%d\r\n%s", stream_count, result)
}

func (server *Redis) handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if n == 0 { // EOF
			continue
		}
		if err != nil {
			fmt.Println("Error reading from connection: ", err.Error())
			continue
		}
		resp_string := string(buf[:n])
		params := strings.Split(resp_string, "\r\n")

		args := []string{}
		for i := 1; i < len(params); i++ {
			if len(params[i]) > 0 && params[i][0] == '$' {
				args = append(args, params[i+1])
				i++
			}
		}
		cmd := strings.ToUpper(args[0])
		resp := ""

		switch cmd {
		case "ECHO":
			resp = server.handleECHO(args[1])
		case "PING":
			resp = server.handlePING()
		case "SET":
			resp = server.handleSET(args[1:])
		case "GET":
			resp = server.handleGET(args[1])
		case "RPUSH":
			resp = server.handleRPUSH(args[1:])
		case "LRANGE":
			resp = server.handleLRANGE(args[1:])
		case "LPUSH":
			resp = server.handleLPUSH(args[1:])
		case "LLEN":
			resp = server.handleLLEN(args[1])
		case "LPOP":
			resp = server.handleLPOP(args[1:])
		case "BLPOP":
			resp = server.handleBLPOP(args[1:])
		case "TYPE":
			resp = server.handleTYPE(args[1:])
		case "XADD":
			resp = server.handleXADD(args[1:])
		case "XRANGE":
			resp = server.handleXRANGE(args[1:])

		default:
			resp = "err\r\n"
		}
		_, err = conn.Write([]byte(resp))

		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error())
			return
		}
	}
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	server := NewRedis()
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go server.handleConnection(conn)
	}
}
