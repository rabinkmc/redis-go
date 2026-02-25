package main

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

func HandleINFO(server *Redis, cmd Command) {
	server.mu.Lock()
	defer server.mu.Unlock()
	info_key := cmd.Args[1]
	section_info, exists := server.info[info_key]
	if !exists {
		cmd.Client.WriteErr("Key doesn't exist")
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
		server.mu.Lock()
		defer server.mu.Unlock()
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
		server.mu.Lock()
		server.slaves = append(server.slaves, conn)
		server.mu.Unlock()
		cmd.Client.WriteStatus("OK")
		return
	}
	if cmdName == "ACK" {
		offset, err := strconv.Atoi(cmd.Args[2])
		if err != nil {
			log.Fatalf("Error parsing integer %v", err)
		}
		server.mu.Lock()
		server.ack_slaves[conn] = offset
		var toSignal []*WaitRequest
		for _, w := range server.replica_waiters {
			if server.slave_sync_count(w.offset) >= w.required {
				toSignal = append(toSignal, w)
			}
		}
		// cleanup waiters
		for _, w := range toSignal {
			server.removeWaiter(w)
		}
		server.mu.Unlock()

		for _, w := range toSignal {
			select {
			case w.ch <- struct{}{}:
			default:
			}
		}
		return
	}
	cmd.Client.WriteStatus("OK")
}

func HandlePSYNC(server *Redis, cmd Command) {
	server.mu.Lock()
	args := cmd.Args[1:]
	if cmd.Args[1] == "?" {
		args[1] = server.id
	}
	if args[2] == "-1" {
		args[2] = "0"
	}
	resp := fmt.Sprintf("FULLRESYNC %s %s", args[1], args[2])
	server.mu.Unlock()
	cmd.Client.WriteStatus(resp)
	// this is a simply sending empty rdb content over the network
	server.writeFile(cmd.Client.conn)
}

func HandleWAIT(server *Redis, cmd Command) { // this is master server waiting for the slaves to catch up
	args := cmd.Args[1:]
	replicas, _ := strconv.Atoi(args[0])
	server.mu.Lock()
	wait_offset := server.master_offset
	count := server.slave_sync_count(wait_offset)
	if count >= replicas || wait_offset == 0 {
		server.mu.Unlock()
		cmd.Client.WriteInt(count)
		return
	}

	w := &WaitRequest{
		required: replicas,
		// this is the master offset, at least all the slaves have to reach this point
		offset: wait_offset,
		ch:     make(chan struct{}),
	}
	server.replica_waiters = append(server.replica_waiters, w)

	slaveConns := append([]net.Conn(nil), server.slaves...)
	server.mu.Unlock()

	request := []byte(encode_list([]string{"REPLCONF", "GETACK", "*"}))
	server.master_offset += len(request)
	for _, slave_conn := range slaveConns {
		_, err := slave_conn.Write(request)
		if err != nil {
			log.Printf("Error writing to the connection: %v", err)
		}
		// just ignore the slave connection that we failed to establish
	}
	// wait for signal or timeout
	mtime, _ := strconv.ParseFloat(args[1], 64)
	timeout := time.Duration(mtime) * time.Millisecond

	select {
	case <-w.ch:
		cmd.Client.WriteInt(replicas)
	case <-time.After(timeout):
		server.mu.Lock()
		finalCount := server.slave_sync_count(w.offset)
		server.removeWaiter(w)
		server.mu.Unlock()
		cmd.Client.WriteInt(finalCount)
	}
}

func HandleCONFIG(server *Redis, cmd Command) {
	server.mu.Lock()
	args := cmd.Args[1:]
	key := args[1]
	switch cmd.Args[0] {
	case "GET":
		if key == "dir" {
			server.mu.Unlock()
			cmd.Client.WriteList([]string{"dir", server.rdb_dir})
			return
		}
		if key == "dbfilename" {
			server.mu.Unlock()
			cmd.Client.WriteList([]string{"dir", server.dbfilename})
			return
		}
	default:
		server.mu.Unlock()
		cmd.Client.WriteNil()
	}
	server.mu.Unlock()
	cmd.Client.WriteNil()
}

func (server *Redis) HandleReplConnection(conn net.Conn, handshake chan string) {
	client := NewClient(conn)
	defer client.Close()
	cmd := &Command{Client: client}
	offset := 0
	reader := bufio.NewReader(conn)
	for {
		typeByte, err := reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				log.Println("Master closed the connection")
			} else {
				log.Printf("Connection error: %v\n", err)
			}
			return
		}

		switch typeByte {
		case '+':
			handleSimpleString(reader, handshake)
		case '$':
			// just read rdb file and discard
			// todo: why did I discard the rdb ?
			handleRDB(reader)
		case '*':
			handlePropagation(server, reader, cmd, &offset)
			// propagation commands
		}
	}
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

func handleSimpleString(reader *bufio.Reader, handshake chan string) {
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
		return
	}
}

func handleRDB(reader *bufio.Reader) {
	line, err := reader.ReadString('\n')
	line = strings.TrimSuffix(line, "\r\n")
	rdb_size, err := strconv.Atoi(line)
	if err != nil {
		log.Printf("Invalid RDB size: %v\n", err)
		return
	}
	buf := make([]byte, rdb_size)
	_, err = io.ReadFull(reader, buf)
	if err != nil {
		log.Printf("Failed to read full RDB: %v\n", err)
		return
	}
}

func handlePropagation(
	server *Redis,
	reader *bufio.Reader,
	cmd *Command,
	offset *int,
) {
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

	cmdName := strings.ToUpper(args[0])
	if cmdName == "REPLCONF" && len(args) > 1 && strings.ToUpper(args[1]) == "GETACK" {
		resp := encode_list([]string{"REPLCONF", "ACK", strconv.Itoa(*offset)})
		cmd.Client.Write([]byte(resp))
	} else if cmdName != "DISCARD" && cmdName != "EXEC" && cmd.Client.queue {
		cmd.Client.commands = append(cmd.Client.commands, args)
	} else {
		cmd.Args = args
		Execute(server, *cmd)
	}
	*offset = *offset + curr_length
}

func (server *Redis) removeWaiter(target *WaitRequest) {
	newWaiters := server.replica_waiters[:0]
	for _, w := range server.replica_waiters {
		if w != target {
			newWaiters = append(newWaiters, w)
		}
	}
	server.replica_waiters = newWaiters
}
