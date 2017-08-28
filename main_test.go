package main

import "testing"

func TestDbVars(t *testing.T) {
	result := DbVars()
	if result != "bar" {
		t.Errorf("expecting bar, got %s", result)
		kh
	}
}
