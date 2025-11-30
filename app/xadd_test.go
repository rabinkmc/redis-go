package main

import (
	"reflect"
	"testing"
)

func TestXADD(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		want string
	}{
		{
			name: "XADD",
			in:   []string{"stream_key", "1-*", "temperature", "36", "humidity", "95"},
			want: encode("1-0"),
		},
		{
			name: "XADD",
			in:   []string{"stream_key", "1-*", "temperature", "36", "humidity", "95"},
			want: encode("1-1"),
		},
		{
			name: "XADD",
			in:   []string{"stream_key", "1-4", "temperature", "36", "humidity", "95"},
			want: encode("1-4"),
		},
		{
			name: "XADD",
			in:   []string{"stream_key", "1-*", "temperature", "36", "humidity", "95"},
			want: encode("1-5"),
		},
		{
			name: "XADD",
			in:   []string{"stream_key", "2-*", "temperature", "36", "humidity", "95"},
			want: encode("2-0"),
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
}
