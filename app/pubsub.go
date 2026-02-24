package main

import (
	"fmt"
	"strings"
)

type Message struct {
	client *Client
	text   string
}

type Topic struct {
	name        string
	subscribers []*Client
	publish     chan Message
	subscribe   chan *Client
	unsubscribe chan *Client
}

func encode_sublist(strs []string, topic_size int) string {
	if len(strs) == 0 {
		return "*0\r\n"
	}
	result := fmt.Sprintf("*%d\r\n", len(strs)+1)
	for _, str := range strs {
		result += fmt.Sprintf("$%d\r\n%s\r\n", len(str), str)
	}
	return result + resp_int(topic_size)
}

func in_subscription_mode(cmd Command) bool {
	if cmd.Client.subscribed {
		return true
	}
	cmdName := cmd.Args[0]
	if cmd.Client.subscribed && cmdName == "PING" {
		return true
	}
	allowed_cmds := []string{
		"SUBSCRIBE", "PSUBSCRIBE",
		"UNSUBSCRIBE", "PUNSUBSCRIBE",
		"PUBLISH",
		"QUIT",
	}
	for _, allowed_cmd := range allowed_cmds {
		if cmdName == allowed_cmd {
			return true
		}
	}
	return false
}

func handleSUBSCRIBE(server *Redis, cmd Command) {
	client := cmd.Client
	topic_str := cmd.Args[1]
	topic, ok := server.pubsub[topic_str]
	if !ok {
		topic = &Topic{
			name:        topic_str,
			publish:     make(chan Message),
			subscribe:   make(chan *Client),
			unsubscribe: make(chan *Client),
		}
		server.pubsub[topic_str] = topic
		client.topics[topic_str] = topic
		go handlePubsub(server, topic)
	}

	select {
	case topic.subscribe <- client:
	}
}

func handleUNSUBSCRIBE(server *Redis, cmd Command) {
	topic_str := cmd.Args[1]
	client := cmd.Client
	topic, ok := server.pubsub[topic_str]
	if !ok {
		return
	}
	select {
	case topic.unsubscribe <- client:
	}
}
func handlePUBLISH(server *Redis, cmd Command) {
	args := cmd.Args[1:]
	client := cmd.Client
	topic_str := args[0]
	topic, ok := server.pubsub[topic_str]
	if !ok {
		return
	}
	message := Message{client: client, text: args[1]}
	select {
	case topic.publish <- message:
	}
}

func handlePubsub(server *Redis, topic *Topic) {
	name := topic.name
	for {
		select {
		case client := <-topic.subscribe:
			topic.subscribers = append(topic.subscribers, client)
			client.subscribed = true
			client.topics[name], server.pubsub[name] = topic, topic
			response := encode_sublist(
				[]string{"subscribe", name},
				len(client.topics),
			)
			client.conn.Write([]byte(response))
		case message := <-topic.publish:
			for _, client := range topic.subscribers {
				response := encode_list([]string{"message", topic.name, message.text})
				client.conn.Write([]byte(response))
			}
			message.client.conn.Write([]byte(resp_int(len(topic.subscribers))))
		case client := <-topic.unsubscribe:
			newsubscribers := topic.subscribers[:0]
			for _, c := range topic.subscribers {
				if c != client {
					newsubscribers = append(newsubscribers, c)
				}

			}
			topic.subscribers = newsubscribers
			server.pubsub[name] = topic
			client.subscribed = false
			delete(client.topics, name)
			response := encode_sublist(
				[]string{"unsubscribe", name},
				len(client.topics),
			)
			client.conn.Write([]byte(response))
		}
	}

}

func handleSubscription(server *Redis, cmd Command) {
	args := cmd.Args[1:]
	client := cmd.Client

	cmdName := strings.ToUpper(args[0])
	switch cmdName {
	case "PING":
		client.conn.Write([]byte(encode_list([]string{"pong", ""})))
	case "SUBSCRIBE":
		handleSUBSCRIBE(server, cmd)
	case "UNSUBSCRIBE":
		handleUNSUBSCRIBE(server, cmd)
	case "PUBLISH":
		handlePUBLISH(server, cmd)
	default:
		resp_err := simple_err(fmt.Sprintf("Can't execute '%s'", cmd))
		client.conn.Write([]byte(resp_err))
	}

}
