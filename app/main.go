package main

import (
	"fmt"
	"net"
	"os"
	"strings"
)

var dict = make(map[string]string)

func encode(str string) string {
	result := fmt.Sprintf("$%d\r\n%s\r\n", len(str), str)
	return result
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
		fmt.Println(args)
		cmd := args[0]

		if cmd == "ECHO" {
			fmt.Println(encode(args[1]))
			_, err = conn.Write([]byte(encode(args[1])))
		} else if cmd == "PING" {
			_, err = conn.Write([]byte("+PONG\r\n"))
		} else if cmd == "SET" {
			key := args[1]
			value := args[2]
			dict[key] = value
			_, err = conn.Write([]byte("+OK\r\n"))
		} else if cmd == "GET" {
			key := args[1]
			val, ok := dict[key]
			if !ok {
				_, err = conn.Write([]byte("$-1\r\n"))
			}
			_, err = conn.Write([]byte(encode(val)))
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
