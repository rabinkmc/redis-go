package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

type Command struct {
	Client *Client
	Args   []string
}

type RedisConfig struct {
	port       int
	replicaof  string
	rdb_dir    string
	dbfilename string
}

type Stream struct {
	id    string
	time  int64
	seq   int64
	items []string // key, val pair
}

type Znode struct {
	member string
	score  float64
}

type Zset []Znode

type User struct {
	username string
	hash     map[string]struct{}
}

func (user *User) nopass() bool {
	return len(user.hash) == 0
}

type Entry struct {
	val      string
	list     []string
	streams  []Stream
	streamMS map[int64]int
	time     *time.Time
	zset     Zset
}

type WaitRequest struct {
	required int
	offset   int
	ch       chan struct{}
}

type Redis struct {
	id              string
	port            int
	dict            map[string]Entry
	mu              sync.RWMutex
	waiters         map[string]chan string
	stream_waiters  map[string]chan string
	info            map[string]map[string]string
	replicaof       string
	master_addr     string
	slaves          []net.Conn
	replica_waiters []*WaitRequest
	ack_slaves      map[net.Conn]int
	master_offset   int
	rdb_dir         string
	dbfilename      string
	db_index        uint32
	key_size        uint32
	exp_key_size    uint32
	rdb_read_status bool
	pubsub          map[string]*Topic
	users           map[string]*User
}

func NewRedis(redis_config RedisConfig) *Redis {
	parts := strings.Split(redis_config.replicaof, " ")
	replication := make(map[string]string)
	replication["master_replid"] = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	replication["master_repl_offset"] = "0"
	replication["role"] = "master"
	if len(parts) == 2 {
		replication["role"] = "slave"
	}

	redis := &Redis{
		id:             "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		replicaof:      redis_config.replicaof,
		port:           redis_config.port,
		dict:           make(map[string]Entry),
		waiters:        make(map[string]chan string),
		stream_waiters: make(map[string]chan string),
		ack_slaves:     make(map[net.Conn]int),
		pubsub:         make(map[string]*Topic),
		users:          make(map[string]*User),
	}

	redis.users["default"] = &User{
		username: "default",
		hash:     make(map[string]struct{}),
	}
	redis.info = make(map[string]map[string]string)
	redis.rdb_dir = redis_config.rdb_dir
	redis.dbfilename = redis_config.dbfilename
	if redis.rdb_dir != "" && redis.dbfilename != "" {
		redis.readRDB()
	}
	redis.info["replication"] = replication
	if replication["role"] == "slave" {
		redis.master_addr = fmt.Sprintf("%s:%s", parts[0], parts[1])
		// initial handshake
		conn, err := net.Dial("tcp", redis.master_addr)
		if err != nil {
			log.Fatalf("Unable to connect to the master: %v", err.Error())
		}
		handshake_ch := make(chan string)
		go redis.HandleReplConnection(conn, handshake_ch) // replica reads from this connection
		send_ping(conn)
		<-handshake_ch
		send_replconf(
			conn,
			[]string{
				"REPLCONF",
				"listening-port",
				fmt.Sprintf("%d", redis.port),
			},
		)
		<-handshake_ch
		send_replconf(
			conn,
			[]string{"REPLCONF", "capa", "psync2"},
		)
		<-handshake_ch
		send_psync(conn, "?", "-1")
	}
	return redis
}

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
	// job of dispatch is to
	// redirect the cmd to right method
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
	// This replaces Basic Commands, HandleGeo, HandleZset, HandleACL
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
