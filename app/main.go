package main

import (
	"fmt"
	"log"
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
	conn net.Conn
	dict map[string]Entry
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

func RPUSH(key string, values []string) int {
	entry := dict[key]
	entry.list = append(entry.list, values...)
	dict[key] = entry
	return len(entry.list)
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if n == 0 { // EOF
			return
		}
		if err != nil {
			fmt.Println("Error reading from connection: ", err.Error())
			return
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
		log.Println(args)
		cmd := strings.ToUpper(args[0])

		if cmd == "ECHO" {
			fmt.Println(encode(args[1]))
			_, err = conn.Write([]byte(encode(args[1])))
		} else if cmd == "PING" {
			_, err = conn.Write([]byte("+PONG\r\n"))
		} else if cmd == "SET" {
			key := args[1]
			value := args[2]
			entry := Entry{val: value}
			if len(args) >= 5 {
				ex_time := time.Now()
				if args[3] == "PX" {
					ms, _ := strconv.Atoi(args[4])
					ex_time = ex_time.Add(time.Duration(ms) * time.Millisecond)
				}
				if args[3] == "EX" {
					sec, _ := strconv.Atoi(args[4])
					ex_time = ex_time.Add(time.Duration(sec) * time.Second)
				}
				entry.time = &ex_time
			}
			dict[key] = entry
			_, err = conn.Write([]byte("+OK\r\n"))
		} else if cmd == "GET" {
			key := args[1]
			entry, ok := dict[key]
			fmt.Printf("time: %v", entry.time)
			if !ok || (entry.time != nil && entry.time.Before(time.Now())) {
				_, err = conn.Write([]byte("$-1\r\n"))
				return
			}
			_, err = conn.Write([]byte(encode(entry.val)))
		} else if cmd == "RPUSH" {
			n = RPUSH(args[1], args[2:])
			resp := fmt.Sprintf(":%d\r\n", n)
			conn.Write([]byte(resp))
		} else if cmd == "LRANGE" {
			empty_arr := []byte("*0\r\n")
			key := args[1]
			entry, ok := dict[key]
			if !ok {
				_, err = conn.Write(empty_arr)
				return
			}

			start, _ := strconv.Atoi(args[2])
			end, _ := strconv.Atoi(args[3])
			n := len(entry.list)

			if (start > end) || (start > n) {
				_, err = conn.Write(empty_arr)
				return
			}

			if end >= n {
				end = n - 1
			}
			lrange := entry.list[start : end+1]

			resp := encode_list(lrange)
			_, err = conn.Write([]byte(resp))
		}

		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error())
			return
		}
	}
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
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
		go handleConnection(conn)
	}
}
