package health

import (
	"context"
	"testing"
	"time"

	"github.com/Zaynereo/mysql-shard-proxy/internal/config"
	"github.com/Zaynereo/mysql-shard-proxy/internal/testutil/mysqlfake"
)

func TestProbeMySQLPrimaryAndReplica(t *testing.T) {
	srv, err := mysqlfake.Start("probe", "secret")
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	backend := config.Backend{Name: "b", Addr: srv.Addr, User: "probe", Password: "secret"}
	ctx := context.Background()
	if _, err := ProbeMySQL(ctx, backend, false, time.Second); err != nil {
		t.Fatal(err)
	}
	srv.SetReplica(true, 2*time.Second)
	sample, err := ProbeMySQL(ctx, backend, true, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !sample.ReplicationRunning || !sample.LagKnown || sample.Lag != 2*time.Second {
		t.Fatalf("sample %+v", sample)
	}
	srv.SetReplica(false, 0)
	sample, err = ProbeMySQL(ctx, backend, true, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if sample.ReplicationRunning || sample.LagKnown {
		t.Fatalf("stopped replica looked healthy: %+v", sample)
	}
}
