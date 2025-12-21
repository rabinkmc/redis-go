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
	b.WriteString(resp_bulk_string("flags"))
	if user.hash == "" {
		b.WriteString(encode_list([]string{"nopass"}))
	} else {
		b.WriteString(EMPTY_ARRAY)
	}
	b.WriteString(resp_bulk_string("passwords"))
	if user.hash == "" {
		b.WriteString(EMPTY_ARRAY)
	} else {
		b.WriteString(encode_list([]string{user.hash}))
	}
	client.conn.Write([]byte(b.String()))
}

func (server *Redis) handleSETUSER(client *Client, args []string) {
	username := args[0]
	user, _ := server.users[username]

	if user == nil {
		user = &User{username: username}
	}
	password := []byte(args[1][1:])
	hash := sha256.Sum256(password)
	user.hash = hex.EncodeToString(hash[:])
	server.users[username] = user
	client.conn.Write([]byte("+OK\r\n"))
}

func is_acl_cmd(cmd string) bool {
	return cmd == "ACL"
}
func (server *Redis) handleACL(client *Client, args []string) {
	cmd := strings.ToUpper(args[1])
	switch cmd {
	case "WHOAMI":
		server.handleWHOAMI(client, args[2:])
	case "GETUSER":
		server.handleGETUSER(client, args[2:])
	case "SETUSER":
		server.handleSETUSER(client, args[2:])
	default:
		client.conn.Write([]byte(simple_err("shouldn't be here in acl")))
	}
}
