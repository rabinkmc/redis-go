package main

import (
	"fmt"
	"log"
)

type Topic struct {
	name        string
	subscribers []*Client
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

func (server *Redis) handleSUBSCRIBE(client *Client, args []string) string {
	topic_str := args[0]
	topic, ok := client.topics[topic_str]
	if ok {
		// already subscribed
		return resp_int(len(client.topics))
	}
	topic, ok = server.pubsub[topic_str]
	if !ok {
		topic = &Topic{name: topic_str}
		log.Printf("New topic created: %s", topic_str)
	}
	topic.subscribers = append(topic.subscribers, client)
	client.topics[topic_str] = topic
	log.Printf("client connected to topic: %s", topic_str)

	return encode_sublist(
		[]string{"SUBSCRIBE", topic_str},
		len(client.topics),
	)

}
