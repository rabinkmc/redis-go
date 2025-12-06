package main

import (
	"testing"
)

func TestINCR(t *testing.T) {

	server := NewRedis()

	entry := Entry{val: "5"}
	server.dict["foo"] = entry

	got := server.Execute([]string{"INCR", "foo"})
	if got != resp_int(6) {
		t.Fatalf("Invalid, %#v", got)
	}

}
