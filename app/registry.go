package main

import (
	"fmt"
	"strings"
)

type CommandHandler func(server *Redis, cmd Command)

type CommandRegistration struct {
	Handler  CommandHandler
	ReadOnly bool
	MinArgs  int
}

var CommandRegistry = make(map[string]CommandRegistration)

func HandleXREADProxy(server *Redis, cmd Command) {
	if len(cmd.Args) > 1 && strings.ToUpper(cmd.Args[1]) == "BLOCK" {
		HandleXREADBLOCK(server, cmd)
	} else {
		HandleXREAD(server, cmd)
	}
}

func HandleACLProxy(server *Redis, cmd Command) {
	if len(cmd.Args) < 2 {
		cmd.Client.WriteErr("ERR wrong number of arguments for 'ACL' command")
		return
	}
	subcmd := strings.ToUpper(cmd.Args[1])
	switch subcmd {
	case "WHOAMI":
		HandleWHOAMI(server, cmd)
	case "GETUSER":
		HandleGETUSER(server, cmd)
	case "SETUSER":
		HandleSETUSER(server, cmd)
	default:
		cmd.Client.WriteErr(fmt.Sprintf("ERR Unknown ACL subcommand %s", subcmd))
	}
}

func init() {
	commands := map[string]CommandRegistration{
		// Basic & Strings
		"PING": {HandlePING, true, 1},
		"ECHO": {HandleECHO, true, 2},
		"SET":  {HandleSET, false, 3},
		"GET":  {HandleGET, true, 2},
		"INCR": {HandleINCR, false, 2},
		"TYPE": {HandleTYPE, true, 2},
		"KEYS": {HandleKEYS, true, 2},

		// Lists
		"LPUSH":  {HandleLPUSH, false, 3},
		"RPUSH":  {HandleRPUSH, false, 3},
		"LPOP":   {HandleLPOP, false, 2},
		"LLEN":   {HandleLLEN, true, 2},
		"LRANGE": {HandleLRANGE, true, 4},
		"BLPOP":  {HandleBLPOP, false, 3},

		// Sorted Sets & Streams
		"ZADD":   {HandleZADD, false, 4},
		"ZREM":   {HandleZREM, false, 3},
		"ZRANK":  {HandleZRANK, true, 3},
		"ZRANGE": {HandleZRANGE, true, 4},
		"ZCARD":  {HandleZCARD, true, 2},
		"ZSCORE": {HandleZSCORE, true, 3},
		"XADD":   {HandleXADD, false, 4},
		"XRANGE": {HandleXRANGE, true, 4},
		"XREAD":  {HandleXREADProxy, true, 3},

		// Geospatial
		"GEOADD":    {HandleGEOADD, false, 5},
		"GEOPOS":    {HandleGEOPOS, true, 3},
		"GEODIST":   {HandleGEODIST, true, 4},
		"GEOSEARCH": {HandleGEOSEARCH, true, 5},

		// Transactions & Auth
		"MULTI":   {HandleMULTI, false, 1},
		"EXEC":    {HandleEXEC, false, 1},
		"DISCARD": {HandleDISCARD, false, 1},
		"AUTH":    {HandleAUTH, false, 3},
		"ACL":     {HandleACLProxy, false, 2},
		"WHOAMI":  {HandleWHOAMI, true, 1},

		// System
		"INFO":     {HandleINFO, true, 2},
		"CONFIG":   {HandleCONFIG, true, 3},
		"WAIT":     {HandleWAIT, false, 3},
		"PSYNC":    {HandlePSYNC, true, 3},
		"REPLCONF": {HandleREPLCONF, false, 3},
	}

	for k, v := range commands {
		CommandRegistry[k] = v
	}
}
