package main

import "testing"

func TestMULTI(t *testing.T) {
	server := NewRedis(6389, "")
	got := server.Execute([]string{"MULTI"})
	if got != "+OK\r\n" {
		t.Fatalf("Error, %#v", got)
	}
}
