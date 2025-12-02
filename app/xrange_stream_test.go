package main

import (
	"reflect"
	"testing"
)

func TestStream(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		want string
	}{
		{
			name: "XADD1",
			in:   []string{"some_key", "1526985054069-0", "temperature", "36", "humidity", "95"},
			want: encode("1526985054069-0"),
		},
		{
			name: "XADD2",
			in:   []string{"some_key", "1526985054079-0", "temperature", "37", "humidity", "94"},
			want: encode("1526985054079-0"),
		},
	}

	server := NewRedis()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := server.handleXADD(tt.in)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("server.HandleXADD(%q)\n %#v; want %#v", tt.in, got, tt.want)
			}
		})
	}
	t.Run("stream case", func(t *testing.T) {
		in := []string{"some_key", "1526985054069", "1526985054079"}
		got := server.handleXRANGE(in)
		want := "*2\r\n" +
			"*2\r\n" +
			"$15\r\n" +
			"1526985054069-0\r\n" +
			"*4\r\n" +
			"$11\r\n" +
			"temperature\r\n" +
			"$2\r\n" +
			"36\r\n" +
			"$8\r\n" +
			"humidity\r\n" +
			"$2\r\n" +
			"95\r\n" +
			"*2\r\n" +
			"$15\r\n" +
			"1526985054079-0\r\n" +
			"*4\r\n" +
			"$11\r\n" +
			"temperature\r\n" +
			"$2\r\n" +
			"37\r\n" +
			"$8\r\n" +
			"humidity\r\n" +
			"$2\r\n" +
			"94\r\n"
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("XRANGE(%q)\n\n %#v\n\n want\n\n %#v", in, got, want)
		}
	})
}
