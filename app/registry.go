package main

type HandlerFunc func(server *Redis, cmd Command)

type CommandDefinition struct {
	Handler  HandlerFunc
	ReadOnly bool
}
