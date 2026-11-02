// Package shard maps shard-key values onto a fixed set of shards.
package shard

import (
	"fmt"
	"hash/fnv"
)

// Index returns a stable shard ordinal in [0, n) for key.
// The function is FNV-1a 32-bit modulo n. It is not a consistent hash:
// changing n moves most keys.
func Index(key string, n int) (int, error) {
	if n <= 0 {
		return 0, fmt.Errorf("shard count must be positive")
	}
	h := fnv.New32a()
	if _, err := h.Write([]byte(key)); err != nil {
		return 0, fmt.Errorf("hash shard key: %w", err)
	}
	return int(h.Sum32() % uint32(n)), nil
}
