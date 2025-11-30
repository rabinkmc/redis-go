package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Stream struct {
	id    string
	items map[string]string
}

type Entry struct {
	val    string
	list   []string
	stream []Stream
	time   *time.Time
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
	} else if len(entry.stream) > 0 {
		resp = "stream"
	} else if len(entry.list) > 0 {
		resp = "list"
	}
	return fmt.Sprintf("+%s\r\n", resp)
}
func (server *Redis) handleXADD(args []string) string {
	fmt.Println(args)
	stream_key := args[0]
	id := args[1]
	n := len(args)
	i := 2
	entry, _ := server.dict[stream_key]
	items := make(map[string]string)

	for i < n {
		key := args[i]
		val := args[i+1]
		items[key] = val
		i = i + 2
	}
	entry.stream = append(entry.stream, Stream{id: id, items: items})
	server.dict[stream_key] = entry

	return encode(id)
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
			fmt.Println(args)
			resp = server.handleXADD(args[1:])
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
