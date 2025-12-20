package main

import (
	"fmt"
	"strconv"
	"strings"
)

func (server *Redis) handleGEOADD(client *Client, args []string) {
	key := args[0]
	latitude, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		resp := simple_err(
			fmt.Sprintf("failed to parse '%s' to float", args[1]),
		)

		client.conn.Write([]byte(resp))
		return
	}
	longitude, err := strconv.ParseFloat(args[2], 64)
	if err != nil {
		resp := simple_err(
			fmt.Sprintf("failed to parse '%s' to float", args[2]),
		)

		client.conn.Write([]byte(resp))
		return
	}
	name := args[3]
	entry, _ := server.dict[key]
	entry.locations = append(
		entry.locations,
		Location{
			longitude: longitude,
			latitude:  latitude,
			name:      name,
		},
	)
	server.dict[key] = entry
	client.conn.Write([]byte(resp_int(len(entry.locations))))
}

func is_geo_cmd(cmd string) bool {
	commands := []string{"GEOADD"}
	for _, command := range commands {
		if command == cmd {
			return true
		}
	}
	return false
}
func (server *Redis) handleGeo(client *Client, args []string) {
	cmd := strings.ToUpper(args[0])
	switch cmd {
	case "GEOADD":
		server.handleGEOADD(client, args[1:])
	default:
		client.conn.Write([]byte(simple_err("shouldn't be here in geo")))
	}
}
