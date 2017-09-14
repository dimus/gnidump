package util

import "testing"

func TestCheck(t *testing.T) {
	var e error
	e = nil
	Check(e)
}
