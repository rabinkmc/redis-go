package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
)

func (server *Redis) authenticate(client *Client, cmdName string) bool {
	// 1. Initialize user if not set
	if client.user == nil {
		client.user = server.users["default"]
	}

	// 2. Bypass check for AUTH command or nopass users
	if client.user.nopass() || client.auth || cmdName == "AUTH" {
		return true
	}

	// 3. Deny access
	client.conn.Write([]byte("-NOAUTH Authentication required\r\n"))
	return false
}

func (server *Redis) dispatch(client *Client, args []string) {
	cmdName := strings.ToUpper(args[0])
	command := Command{Client: client, Args: args}
	// 1. Transaction Queueing Logic
	// We queue everything except control commands when in MULTI mode
	if client.queue && cmdName != "EXEC" && cmdName != "DISCARD" && cmdName != "MULTI" {
		client.commands = append(client.commands, args)
		client.WriteStatus("QUEUED")
		return
	}

	// 2. Specialized Mode Logic (e.g., Pub/Sub)
	// Commands like SUBSCRIBE change the connection state
	if in_subscription_mode(command) {
		HandleSubscription(server, command)
		return
	}

	// 3. Centralized Registry Execution
	// This replaces HandleGeo, HandleZset, HandleACL, etc.
	Execute(server, command)

	// 4. Replication Propagation
	// Only propagate write commands (handled inside Execute or here based on registry flag)
	if reg, ok := CommandRegistry[cmdName]; ok && !reg.ReadOnly {
		server.write_slaves(cmdName, []byte(encode_list(args)))
	}
}

func Execute(server *Redis, cmd Command) {
	if len(cmd.Args) == 0 {
		return
	}

	cmdName := strings.ToUpper(cmd.Args[0])
	reg, ok := CommandRegistry[cmdName]

	// 1. Check if command exists
	if !ok {
		cmd.Client.WriteErr(fmt.Sprintf("ERR unknown command '%s'", cmdName))
		return
	}

	// 2. Validate argument count
	if len(cmd.Args) < reg.MinArgs {
		cmd.Client.WriteErr(
			fmt.Sprintf("ERR wrong number of arguments for '%s' command",
				cmdName,
			),
		)
		return
	}

	reg.Handler(server, cmd)
}

func (server *Redis) HandleConnection(conn net.Conn) {
	client := NewClient(conn)
	defer client.Close()
	reader := NewRespReader(conn)
	for {
		args, err := reader.ReadCommand()
		if err != nil {
			if err != io.EOF {
				log.Printf("error reading command: %v", err)
			}
			break
		}
		cmdName := strings.ToUpper(args[0])
		if !server.authenticate(client, cmdName) {
			continue
		}

		server.dispatch(client, args)
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
