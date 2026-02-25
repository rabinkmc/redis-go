package main

import (
	"fmt"
	"strings"
)

func HandleMULTI(server *Redis, cmd Command) {
	client := cmd.Client
	client.queue = true
	cmd.Client.WriteStatus("OK")
}

func HandleDISCARD(server *Redis, cmd Command) {
	client := cmd.Client
	if !client.queue {
		cmd.Client.WriteErr("DISCARD without MULTI")
	}
	client.queue = false
	client.commands = [][]string{}
	cmd.Client.WriteStatus("OK")
}

func HandleEXEC(server *Redis, cmd Command) {
	client := cmd.Client
	if !client.queue {
		client.WriteErr("EXEC without MULTI")
		return
	}
	client.queue = false

	client.WriteString(fmt.Sprintf("*%d\r\n", len(client.commands)))

	for _, args := range client.commands {
		cmdName := strings.ToUpper(args[0])
		reg, ok := CommandRegistry[cmdName]

		if !ok {
			client.WriteErr("ERR unknown command in EXEC")
			continue
		}
		// We are already inside a Lock from the EXEC call,
		// so we call the handler directly.
		reg.Handler(server, Command{Client: client, Args: args})
	}
	client.commands = [][]string{}
}
