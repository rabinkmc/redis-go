package main

import "strings"

func (server *Redis) handleWHOAMI(client *Client, args []string) {
	client.conn.Write([]byte(resp_bulk_string("default")))
}

func is_acl_cmd(cmd string) bool {
	return cmd == "ACL"
}
func (server *Redis) handleACL(client *Client, args []string) {
	cmd := strings.ToUpper(args[1])
	switch cmd {
	case "WHOAMI":
		server.handleWHOAMI(client, args[2:])
	default:
		client.conn.Write([]byte(simple_err("shouldn't be here in acl")))
	}
}
