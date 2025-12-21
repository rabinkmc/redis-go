package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

func (server *Redis) handleWHOAMI(client *Client, args []string) {
	client.conn.Write([]byte(resp_bulk_string("default")))
}

func (server *Redis) handleGETUSER(client *Client, args []string) {
	username := args[0]
	user, _ := server.users[username]
	if user == nil {
		resp := fmt.Sprintf("No user exists with username '%s'", username)
		client.conn.Write([]byte(simple_err(resp)))
		return
	}
	var b strings.Builder
	b.WriteString("*4\r\n")
	//1
	b.WriteString(resp_bulk_string("flags"))
	if len(user.hash) == 0 {
		//2
		b.WriteString(encode_list([]string{"nopass"}))
	} else {
		//2
		b.WriteString(EMPTY_ARRAY)
	}
	//3
	b.WriteString(resp_bulk_string("passwords"))
	hashes := []string{}
	for hash := range user.hash {
		hashes = append(hashes, hash)

	}
	//4
	b.WriteString(encode_list(hashes))
	client.conn.Write([]byte(b.String()))
}

func get_hash(pass string) string {
	password := []byte(pass)
	hash := sha256.Sum256(password)
	return hex.EncodeToString(hash[:])
}

func (server *Redis) handleSETUSER(client *Client, args []string) {
	username := args[0]
	user, _ := server.users[username]

	if user == nil {
		user = &User{username: username, hash: make(map[string]struct{})}
	}
	// excluding >
	password := args[1][1:]
	user.hash[get_hash(password)] = struct{}{}
	server.users[username] = user
	client.conn.Write([]byte("+OK\r\n"))
}

func (server *Redis) handleAUTH(client *Client, args []string) {
	username := args[0]
	password := args[1]
	user, _ := server.users[username]
	if user == nil {
		resp_err := "-WRONGPASS invalid username-password pair or user is disabled\r\n"
		client.conn.Write([]byte(resp_err))
		return
	}
	hash := get_hash(password)
	if _, exists := user.hash[hash]; !exists {
		resp_err := "-WRONGPASS invalid username-password pair or user is disabled\r\n"
		client.conn.Write([]byte(resp_err))
	} else {
		client.user = user
		client.auth = true
		client.conn.Write([]byte("+OK\r\n"))
	}
}

func is_acl_cmd(cmd string) bool {
	return cmd == "ACL" || cmd == "AUTH"
}
func (server *Redis) handleACL(client *Client, args []string) {
	subcmd := strings.ToUpper(args[1])
	cmd := strings.ToUpper(args[0])

	switch {
	case subcmd == "WHOAMI":
		server.handleWHOAMI(client, args[2:])
	case subcmd == "GETUSER":
		server.handleGETUSER(client, args[2:])
	case subcmd == "SETUSER":
		server.handleSETUSER(client, args[2:])
	case cmd == "AUTH":
		server.handleAUTH(client, args[1:])
	default:
		client.conn.Write([]byte(simple_err("shouldn't be here in acl")))
	}
}
