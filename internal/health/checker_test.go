package health

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Zaynereo/mysql-shard-proxy/internal/config"
)

func TestCheckerLagAndRecovery(t *testing.T) {
	backend := config.Backend{Name: "r0", Addr: "127.0.0.1:1", User: "u"}
	var muLag time.Duration
	var muErr error
	prober := func(context.Context, config.Backend, bool) (Sample, error) {
		if muErr != nil {
			return Sample{}, muErr
		}
		return Sample{ReplicationRunning: true, Lag: muLag, LagKnown: true}, nil
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := New([]Target{{Shard: "shard0", Backend: backend, Replica: true}}, prober, config.HealthConfig{
		Interval:         config.Duration{Duration: time.Hour},
		Timeout:          config.Duration{Duration: time.Second},
		LagThreshold:     config.Duration{Duration: time.Second},
		FailureThreshold: 2,
		SuccessThreshold: 2,
	}, log)

	c.observe(c.targets[0], Sample{ReplicationRunning: true, Lag: 0, LagKnown: true}, nil)
	if c.Healthy("shard0", "r0") {
		t.Fatal("one success should not enter rotation when threshold is 2")
	}
	c.observe(c.targets[0], Sample{ReplicationRunning: true, Lag: 0, LagKnown: true}, nil)
	if !c.Healthy("shard0", "r0") {
		t.Fatal("expected replica in rotation")
	}
	c.observe(c.targets[0], Sample{ReplicationRunning: true, Lag: 5 * time.Second, LagKnown: true}, nil)
	if !c.Healthy("shard0", "r0") {
		t.Fatal("single lag sample should not remove the replica")
	}
	c.observe(c.targets[0], Sample{ReplicationRunning: true, Lag: 5 * time.Second, LagKnown: true}, nil)
	if c.Healthy("shard0", "r0") {
		t.Fatal("lagging replica should leave rotation")
	}
	c.observe(c.targets[0], Sample{}, errors.New("dial failed"))
	c.observe(c.targets[0], Sample{ReplicationRunning: true, Lag: 0, LagKnown: true}, nil)
	if c.Healthy("shard0", "r0") {
		t.Fatal("recovery should require consecutive successes")
	}
	c.observe(c.targets[0], Sample{ReplicationRunning: false, LagKnown: false}, nil)
	snap := c.Snapshot()
	if len(snap) != 1 || snap[0].Up || snap[0].LastErr == "" {
		t.Fatalf("snapshot %+v", snap)
	}
	c.observe(c.targets[0], Sample{ReplicationRunning: true, Lag: 200 * time.Millisecond, LagKnown: true}, nil)
	c.observe(c.targets[0], Sample{ReplicationRunning: true, Lag: 200 * time.Millisecond, LagKnown: true}, nil)
	if !c.Healthy("shard0", "r0") {
		t.Fatal("expected replica to return")
	}
}

func TestPrimariesHealthy(t *testing.T) {
	primary := config.Backend{Name: "p", Addr: "127.0.0.1:1", User: "u"}
	c := New([]Target{{Shard: "s", Backend: primary, Replica: false}}, func(context.Context, config.Backend, bool) (Sample, error) {
		return Sample{}, nil
	}, config.HealthConfig{
		Interval:         config.Duration{Duration: time.Hour},
		Timeout:          config.Duration{Duration: time.Second},
		FailureThreshold: 1,
		SuccessThreshold: 1,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if c.PrimariesHealthy() {
		t.Fatal("primary starts out of rotation")
	}
	c.observe(c.targets[0], Sample{}, nil)
	if !c.PrimariesHealthy() {
		t.Fatal("primary should be healthy")
	}
}
