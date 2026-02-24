package main

import (
	"net"
	"strconv"
)

var (
	ResponseOK    = []byte("+OK\r\n")
	ResponseNil   = []byte("$-1\r\n")
	ResponseEmpty = []byte("*0\r\n")
)

type Client struct {
	conn       net.Conn
	topics     map[string]*Topic
	commands   [][]string
	queue      bool
	subscribed bool
	user       *User
	auth       bool
}

func NewClient(conn net.Conn) *Client {
	client := &Client{conn: conn, topics: make(map[string]*Topic)}
	return client
}

func (client *Client) Close() {
	client.conn.Close()
}

func (client *Client) WriteInt(num int) {
	var b []byte
	b = append(b, ':')
	b = strconv.AppendInt(b, int64(num), 10)
	b = append(b, "\r\n"...)
	client.conn.Write(b)
}

func (client *Client) WriteBulkString(str string) {
	var b []byte
	b = append(b, '$')
	b = strconv.AppendInt(b, int64(len(str)), 10)
	b = append(b, "\r\n"...)
	b = append(b, str...)
	b = append(b, "\r\n"...)
	client.conn.Write(b)
}

func (client *Client) WriteErr(str string) {
	var b []byte
	b = append(b, "-ERR "...)
	b = append(b, str...)
	b = append(b, "\r\n"...)
	client.conn.Write(b)
}

func (client *Client) WriteList(strs []string) {
	var b []byte
	b = append(b, '*')
	b = strconv.AppendInt(b, int64(len(strs)), 10)
	b = append(b, "\r\n"...)
	for _, str := range strs {
		b = append(b, '$')
		b = strconv.AppendInt(b, int64(len(str)), 10)
		b = append(b, "\r\n"...)
		b = append(b, str...)
		b = append(b, "\r\n"...)
	}
	client.conn.Write(b)
}

func (client *Client) WriteNil() {
	client.conn.Write(ResponseNil)
}

func (client *Client) WriteString(str string) {
	client.conn.Write([]byte(str))
}
func (client *Client) Write(b []byte) {
	client.conn.Write(b)
}

func (client *Client) WriteEmpty() {
	client.conn.Write(ResponseEmpty)
}

func (client *Client) WriteStatus(status string) {
	client.conn.Write([]byte("+" + status + "\r\n"))
}
