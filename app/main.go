package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type Entry struct {
	val  string
	list []string
	time *time.Time
}

type Redis struct {
	dict map[string]Entry
}

func NewRedis() *Redis {
	return &Redis{dict: make(map[string]Entry)}
}

var dict = make(map[string]Entry)

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
	if len(args) >= 5 {
		ex_time := time.Now()
		if args[2] == "PX" {
			ms, _ := strconv.Atoi(args[3])
			ex_time = ex_time.Add(time.Duration(ms) * time.Millisecond)
		}
		if args[2] == "EX" {
			sec, _ := strconv.Atoi(args[3])
			ex_time = ex_time.Add(time.Duration(sec) * time.Second)
		}
		entry.time = &ex_time
	}
	dict[key] = entry
	return "+OK\r\n"
}

func (server *Redis) handleGET(key string) string {
	entry, ok := dict[key]
	fmt.Printf("time: %v", entry.time)
	if !ok || (entry.time != nil && entry.time.Before(time.Now())) {
		return "$-1\r\n"
	}

	return encode(entry.val)
}

func (serer *Redis) handleRPUSH(key string, values []string) string {
	entry := dict[key]
	entry.list = append(entry.list, values...)
	dict[key] = entry
	n := len(entry.list)
	resp := fmt.Sprintf(":%d\r\n", n)
	return resp
}

func (server *Redis) handleLRANGE(args []string) string {
	key := args[0]
	empty_arr := "*0\r\n"
	entry, ok := dict[key]
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
	lrange := entry.list[start : end+1]

	resp := encode_list(lrange)
	return resp
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

		if cmd == "ECHO" {
			resp = server.handleECHO(args[1])
		} else if cmd == "PING" {
			resp = server.handlePING()
		} else if cmd == "SET" {
			resp = server.handleSET(args[1:])
		} else if cmd == "GET" {
			resp = server.handleGET(args[1])
		} else if cmd == "RPUSH" {
			resp = server.handleRPUSH(args[1], args[2:])
		} else if cmd == "LRANGE" {
			resp = server.handleLRANGE(args[1:])
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
