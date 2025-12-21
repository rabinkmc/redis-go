package main

import (
	"testing"
)

func TestGeoscore(t *testing.T) {
	latitude, longitude := 27.7017, 85.3206
	got := get_zscore(latitude, longitude)
	expected := int64(3639507404773204)
	if got != expected {
		t.Fatalf("Expected: %d, got %d", expected, got)
	}
}
