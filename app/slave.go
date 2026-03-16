package main

import (
	"log"
	"net"
)

func (server *Redis) removeSlave(conn net.Conn) {
	// remove from slaves list
	newSlaves := server.slaves[:0]
	for _, c := range server.slaves {
		if c != conn {
			newSlaves = append(newSlaves, c)
		}
	}
	server.slaves = newSlaves
	delete(server.ack_slaves, conn)
	log.Println("Slave disconnected:", conn.RemoteAddr())
}

func (server *Redis) slave_sync_count(wait_offset int) int {
	if server.master_offset == 0 {
		return len(server.slaves)
	}
	count := 0
	for _, offset := range server.ack_slaves {
		if offset >= wait_offset {
			count++
		}
	}
	return count
}

func (server *Redis) write_slaves(cmd string, buf []byte) {
	if len(server.slaves) == 0 {
		return
	}
	server.master_offset += len(buf)
	for _, conn := range server.slaves {
		_, err := conn.Write(buf)
		if err != nil {
			log.Printf("Error writing to the %v: %v\n", conn, err.Error())
			server.removeSlave(conn)
			conn.Close()
		}
	}
}

func send_ping(conn net.Conn) {
	_, err := conn.Write([]byte(encode_list([]string{"PING"})))
	if err != nil {
		log.Fatalf(
			"Failed to send PING command to the master: %v",
			err.Error(),
		)
	}
}
func send_replconf(conn net.Conn, request []string) {
	_, err := conn.Write([]byte(encode_list(request)))
	if err != nil {
		log.Fatalf(
			"Error sending replconf:: %v: %v",
			request, err.Error(),
		)
	}
}

func send_psync(conn net.Conn, replication_id, offset string) {
	request := []string{"PSYNC", replication_id, offset}
	_, err := conn.Write([]byte(encode_list(request)))
	if err != nil {
		log.Fatalf(
			"Error sending PSYNC:: %v: %v\n",
			request, err.Error(),
		)
	}
}
