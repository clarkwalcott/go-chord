package util

import (
	"testing"
	"time"
)

func TestRandStabilize(t *testing.T) {
	min := 10 * time.Second
	max := 30 * time.Second

	var times []time.Duration
	for i := 0; i < 1000; i++ {
		after := RandStabilize(min, max)
		times = append(times, after)
		if after < min {
			t.Fatalf("after below min")
		}
		if after > max {
			t.Fatalf("after above max")
		}
	}

	collisions := 0
	for idx, val := range times {
		for i := 0; i < len(times); i++ {
			if idx != i && times[i] == val {
				collisions += 1
			}
		}
	}

	if collisions > 3 {
		t.Fatalf("too many collisions %d", collisions)
	}
}

func TestBetween(t *testing.T) {
	t1 := []byte{0, 0, 0, 0}
	t2 := []byte{1, 0, 0, 0}
	k := []byte{0, 0, 5, 0}
	if !Between(t1, t2, k) {
		t.Fatalf("expected k between!")
	}
	if Between(t1, t2, t1) {
		t.Fatalf("dont expect t1 between!")
	}
	if Between(t1, t2, t2) {
		t.Fatalf("dont expect t1 between!")
	}

	k = []byte{2, 0, 0, 0}
	if Between(t1, t2, k) {
		t.Fatalf("dont expect k between!")
	}
}

func TestBetweenWrap(t *testing.T) {
	t1 := []byte{0xff, 0, 0, 0}
	t2 := []byte{1, 0, 0, 0}
	k := []byte{0, 0, 5, 0}
	if !Between(t1, t2, k) {
		t.Fatalf("expected k between!")
	}

	k = []byte{0xff, 0xff, 0, 0}
	if !Between(t1, t2, k) {
		t.Fatalf("expect k between!")
	}
}

func TestBetweenRightIncl(t *testing.T) {
	t1 := []byte{0, 0, 0, 0}
	t2 := []byte{1, 0, 0, 0}
	k := []byte{1, 0, 0, 0}
	if !BetweenRightIncl(t1, t2, k) {
		t.Fatalf("expected k between!")
	}
}

func TestBetweenRightInclWrap(t *testing.T) {
	t1 := []byte{0xff, 0, 0, 0}
	t2 := []byte{1, 0, 0, 0}
	k := []byte{1, 0, 0, 0}
	if !BetweenRightIncl(t1, t2, k) {
		t.Fatalf("expected k between!")
	}
}

func TestPowerOffset(t *testing.T) {
	id := []byte{0, 0, 0, 0}
	exp := 30
	mod := 32
	val := PowerOffset(id, exp, mod)
	if val[0] != 64 {
		t.Fatalf("unexpected val! %v", val)
	}

	// 0-7, 8-15, 16-23, 24-31
	id = []byte{0, 0xff, 0xff, 0xff}
	exp = 23
	val = PowerOffset(id, exp, mod)
	if val[0] != 1 || val[1] != 0x7f || val[2] != 0xff || val[3] != 0xff {
		t.Fatalf("unexpected val! %v", val)
	}
}

func TestMax(t *testing.T) {
	if Max(-10, 10) != 10 {
		t.Fatalf("bad max")
	}
	if Max(10, -10) != 10 {
		t.Fatalf("bad max")
	}
}

func TestMin(t *testing.T) {
	if Min(-10, 10) != -10 {
		t.Fatalf("bad min")
	}
	if Min(10, -10) != -10 {
		t.Fatalf("bad min")
	}
}
