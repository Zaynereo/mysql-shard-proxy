package shard

import (
	"testing"
)

func TestIndexStable(t *testing.T) {
	a, err := Index("42", 4)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Index("42", 4)
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("hash not stable: %d vs %d", a, b)
	}
	if a < 0 || a >= 4 {
		t.Fatalf("index out of range: %d", a)
	}
}

func TestIndexSpread(t *testing.T) {
	const n = 4
	seen := map[int]int{}
	for i := 0; i < 2000; i++ {
		idx, err := Index(string(rune('a'))+itoa(i), n)
		if err != nil {
			t.Fatal(err)
		}
		seen[idx]++
	}
	if len(seen) != n {
		t.Fatalf("expected all %d shards to receive keys, got %v", n, seen)
	}
	for i, c := range seen {
		if c < 200 {
			t.Fatalf("shard %d received only %d keys", i, c)
		}
	}
}

func TestIndexRejectsEmptySet(t *testing.T) {
	if _, err := Index("x", 0); err == nil {
		t.Fatal("expected error")
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [16]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
