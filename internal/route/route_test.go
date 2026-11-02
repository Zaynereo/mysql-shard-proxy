package route

import (
	"testing"

	"github.com/Zaynereo/mysql-shard-proxy/internal/shard"
)

func TestBuildSingleReadAndWrite(t *testing.T) {
	key := "42"
	idx, err := shard.Index(key, 2)
	if err != nil {
		t.Fatal(err)
	}
	read, err := Build(Input{ShardCount: 2, Sharded: true, Keys: []string{key}, AllowMultiShardRead: true})
	if err != nil {
		t.Fatal(err)
	}
	if read.Role != Replica || len(read.Shards) != 1 || read.Shards[0] != idx {
		t.Fatalf("read plan %+v idx %d", read, idx)
	}
	write, err := Build(Input{ShardCount: 2, Sharded: true, Keys: []string{key}, Write: true})
	if err != nil {
		t.Fatal(err)
	}
	if write.Role != Primary || write.Shards[0] != idx {
		t.Fatalf("write plan %+v", write)
	}
	ryw, err := Build(Input{ShardCount: 2, Sharded: true, Keys: []string{key}, PreferPrimary: true})
	if err != nil {
		t.Fatal(err)
	}
	if ryw.Role != Primary {
		t.Fatalf("ryw role %s", ryw.Role)
	}
}

func TestBuildRejectsMissingKeyAndCrossShardTxn(t *testing.T) {
	if _, err := Build(Input{ShardCount: 2, Sharded: true, All: true, Write: true}); err == nil {
		t.Fatal("expected missing key on write")
	}
	if _, err := Build(Input{ShardCount: 2, Sharded: true, All: true}); err == nil {
		t.Fatal("expected missing key on read")
	}
	scatter, err := Build(Input{ShardCount: 2, Sharded: true, All: true, AllowScatterRead: true})
	if err != nil {
		t.Fatal(err)
	}
	if !scatter.Fanout || len(scatter.Shards) != 2 || !scatter.MergeRows {
		t.Fatalf("scatter %+v", scatter)
	}
	if _, err := Build(Input{ShardCount: 2, Sharded: true, All: true, AllowScatterRead: true, UnsafeMerge: true}); err == nil {
		t.Fatal("expected unsafe scatter rejection")
	}
	idx, err := shard.Index("1", 2)
	if err != nil {
		t.Fatal(err)
	}
	_, err = Build(Input{
		ShardCount: 2,
		Sharded:    true,
		Keys:       []string{"1"},
		InTxn:      true,
		TxnShard:   idx,
		Write:      true,
	})
	if err != nil {
		t.Fatal(err)
	}
	other := 1 - idx
	_, err = Build(Input{
		ShardCount: 2,
		Sharded:    true,
		Keys:       []string{"1"},
		InTxn:      true,
		TxnShard:   other,
		Write:      true,
	})
	if err == nil {
		t.Fatal("expected cross-shard transaction error")
	}
}

func TestBuildInsertSameShardAndDDL(t *testing.T) {
	if _, err := Build(Input{ShardCount: 2, Sharded: true, PerRow: true, Keys: []string{"a", "b"}, Write: true}); err == nil {
		// a and b might collide; only fail the test when they differ and were accepted
		ia, _ := shard.Index("a", 2)
		ib, _ := shard.Index("b", 2)
		if ia != ib {
			t.Fatal("expected multi-shard insert error")
		}
	}
	ddl, err := Build(Input{ShardCount: 2, DDL: true})
	if err != nil {
		t.Fatal(err)
	}
	if ddl.Role != Primary || len(ddl.Shards) != 2 {
		t.Fatalf("ddl %+v", ddl)
	}
	meta, err := Build(Input{ShardCount: 2, PreferPrimary: true})
	if err != nil {
		t.Fatal(err)
	}
	if meta.Shards[0] != 0 || meta.Role != Primary {
		t.Fatalf("meta %+v", meta)
	}
}

func TestBuildMultiRead(t *testing.T) {
	// Find two keys on different shards.
	var keys []string
	seen := map[int]string{}
	for i := 0; i < 50 && len(seen) < 2; i++ {
		k := string(rune('a' + i))
		idx, err := shard.Index(k, 2)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := seen[idx]; !ok {
			seen[idx] = k
			keys = append(keys, k)
		}
	}
	if len(keys) < 2 {
		t.Fatal("could not find two shards")
	}
	if _, err := Build(Input{ShardCount: 2, Sharded: true, Keys: keys}); err == nil {
		t.Fatal("expected multi-shard read to be rejected by default")
	}
	plan, err := Build(Input{ShardCount: 2, Sharded: true, Keys: keys, AllowMultiShardRead: true})
	if err != nil {
		t.Fatal(err)
	}
	if !plan.MergeRows || plan.Role != Replica || len(plan.Shards) != 2 {
		t.Fatalf("plan %+v", plan)
	}
	if _, err := Build(Input{ShardCount: 2, Sharded: true, Keys: keys, AllowMultiShardRead: true, UnsafeMerge: true}); err == nil {
		t.Fatal("expected unsafe multi-shard read rejection")
	}
	write, err := Build(Input{ShardCount: 2, Sharded: true, Keys: keys, Write: true})
	if err != nil {
		t.Fatal(err)
	}
	if write.Role != Primary || !write.Fanout || write.MergeRows {
		t.Fatalf("write fanout %+v", write)
	}
}
