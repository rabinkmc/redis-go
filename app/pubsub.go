package main

import "log"

type Topic struct {
	name        string
	subscribers []*Client
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

	return resp_int(len(client.topics))
}
