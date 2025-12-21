package main

import (
	"fmt"
	"testing"
)

func TestGeoencode(t *testing.T) {
	latitude, longitude := 27.7017, 85.3206
	got := encode_pos(latitude, longitude)
	expected := uint64(3639507404773204)
	if got != expected {
		t.Fatalf("Expected: %d, got %d", expected, got)
	}
}

func TestGeodecode(t *testing.T) {
	zcode := uint64(3639507404773204)
	lat, long := decode_geocode(zcode)
	// latitude, longitude := 27.7017, 85.3206
	fmt.Println(lat, long)
}
