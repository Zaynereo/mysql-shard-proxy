// Package route chooses shard ordinals and a primary or replica role.
package route

import (
	"fmt"

	"github.com/Zaynereo/mysql-shard-proxy/internal/shard"
)

const (
	// Primary is the writable member of a shard.
	Primary = "primary"
	// Replica is a read-only member of a shard.
	Replica = "replica"
)

// Input is the routing request for one statement.
type Input struct {
	ShardCount          int
	Keys                []string
	PerRow              bool
	All                 bool
	Sharded             bool
	Write               bool
	PreferPrimary       bool
	InTxn               bool
	TxnShard            int
	AllowMultiShardRead bool
	AllowScatterRead    bool
	UnsafeMerge         bool
	DDL                 bool
}

// Plan is where a statement runs.
type Plan struct {
	Shards    []int
	Role      string
	Fanout    bool
	MergeRows bool
}

// Build returns the shards and role for a statement.
// TxnShard is ignored when it is negative.
func Build(in Input) (Plan, error) {
	if in.ShardCount < 1 {
		return Plan{}, fmt.Errorf("no shards configured")
	}
	if in.DDL {
		return Plan{Shards: seq(in.ShardCount), Role: Primary, Fanout: true}, nil
	}
	role := Replica
	if in.PreferPrimary || in.InTxn || in.Write {
		role = Primary
	}
	if !in.Sharded {
		idx := 0
		if in.InTxn && in.TxnShard >= 0 {
			idx = in.TxnShard
		}
		return Plan{Shards: []int{idx}, Role: role}, nil
	}
	if in.All {
		if in.InTxn && in.TxnShard >= 0 {
			return Plan{Shards: []int{in.TxnShard}, Role: Primary}, nil
		}
		if in.Write {
			return Plan{}, fmt.Errorf("write is missing shard key")
		}
		if !in.AllowScatterRead || in.UnsafeMerge {
			return Plan{}, fmt.Errorf("query is missing shard key")
		}
		return Plan{Shards: seq(in.ShardCount), Role: role, Fanout: true, MergeRows: true}, nil
	}
	ordered, err := shardIndexes(in.Keys, in.ShardCount)
	if err != nil {
		return Plan{}, err
	}
	if len(ordered) == 0 {
		return Plan{}, fmt.Errorf("query is missing shard key")
	}
	if in.PerRow && len(ordered) > 1 {
		return Plan{}, fmt.Errorf("INSERT rows map to more than one shard")
	}
	if in.InTxn && in.TxnShard >= 0 {
		for _, idx := range ordered {
			if idx != in.TxnShard {
				return Plan{}, fmt.Errorf("cross-shard transactions are not supported")
			}
		}
		return Plan{Shards: []int{in.TxnShard}, Role: Primary}, nil
	}
	if len(ordered) == 1 {
		return Plan{Shards: ordered, Role: role}, nil
	}
	if in.InTxn {
		return Plan{}, fmt.Errorf("cross-shard transactions are not supported")
	}
	if in.Write {
		return Plan{Shards: ordered, Role: Primary, Fanout: true}, nil
	}
	if in.UnsafeMerge || !in.AllowMultiShardRead {
		return Plan{}, fmt.Errorf("multi-shard read is not allowed for this query")
	}
	return Plan{Shards: ordered, Role: role, Fanout: true, MergeRows: true}, nil
}

func shardIndexes(keys []string, n int) ([]int, error) {
	seen := map[int]struct{}{}
	ordered := make([]int, 0, len(keys))
	for _, key := range keys {
		idx, err := shard.Index(key, n)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[idx]; ok {
			continue
		}
		seen[idx] = struct{}{}
		ordered = append(ordered, idx)
	}
	return ordered, nil
}

func seq(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}
