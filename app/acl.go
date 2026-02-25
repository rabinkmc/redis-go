package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

func HandleWHOAMI(server *Redis, cmd Command) {
	client := cmd.Client
	client.conn.Write([]byte(resp_bulk_string("default")))
}

func HandleGETUSER(server *Redis, cmd Command) {
	client := cmd.Client
	username := cmd.Args[0]
	server.mu.Lock()
	defer server.mu.Unlock()
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

func HandleSETUSER(server *Redis, cmd Command) {
	args := cmd.Args[1:]
	username := args[0]
	password := args[1][1:]
	newHash := get_hash(password)

	server.mu.Lock()
	defer server.mu.Unlock()

	user, _ := server.users[username]

	if user == nil {
		user = &User{username: username, hash: make(map[string]struct{})}
	}
	// excluding >
	user.hash[newHash] = struct{}{}
	server.users[username] = user
	cmd.Client.WriteStatus("OK")
}

func HandleAUTH(server *Redis, cmd Command) {
	args := cmd.Args[1:]
	client := cmd.Client
	username := args[0]
	password := args[1]
	hash := get_hash(password)
	server.mu.Lock()
	defer server.mu.Unlock()
	user, _ := server.users[username]
	if user == nil {
		cmd.Client.WriteErr(
			"WRONGPASS invalid username-password pair or user is disabled",
		)
		return
	}
	if _, exists := user.hash[hash]; !exists {
		cmd.Client.WriteErr(
			"WRONGPASS invalid username-password pair or user is disabled",
		)
	} else {
		client.user = user
		client.auth = true
		cmd.Client.WriteStatus("OK")
	}
}
