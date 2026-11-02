// Package health probes primaries and replicas and drops lagging replicas from rotation.
package health

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/Zaynereo/mysql-shard-proxy/internal/config"
)

// Sample is the raw result of one probe.
type Sample struct {
	ReplicationRunning bool
	Lag                time.Duration
	LagKnown           bool
}

// Prober checks one backend. replica selects the replication-lag probe.
type Prober func(ctx context.Context, backend config.Backend, replica bool) (Sample, error)

// Target is one backend the checker watches.
type Target struct {
	Shard   string
	Backend config.Backend
	Replica bool
}

// Status is the latest decision for a backend.
type Status struct {
	Shard    string
	Backend  string
	Replica  bool
	Up       bool
	Lag      time.Duration
	LagKnown bool
	LastErr  string
}

// Checker marks replicas down and restores them after consecutive probes.
type Checker struct {
	targets          []Target
	prober           Prober
	interval         time.Duration
	timeout          time.Duration
	lagThreshold     time.Duration
	failureThreshold int
	successThreshold int
	log              *slog.Logger

	mu     sync.RWMutex
	states map[string]*backendState
}

type backendState struct {
	up       bool
	fails    int
	oks      int
	lag      time.Duration
	lagKnown bool
	lastErr  string
}

// New builds a checker. Backends start out of rotation until a probe succeeds.
func New(targets []Target, prober Prober, cfg config.HealthConfig, log *slog.Logger) *Checker {
	if log == nil {
		log = slog.Default()
	}
	states := make(map[string]*backendState, len(targets))
	for _, t := range targets {
		states[key(t.Shard, t.Backend.Name)] = &backendState{}
	}
	return &Checker{
		targets:          targets,
		prober:           prober,
		interval:         cfg.Interval.Duration,
		timeout:          cfg.Timeout.Duration,
		lagThreshold:     cfg.LagThreshold.Duration,
		failureThreshold: cfg.FailureThreshold,
		successThreshold: cfg.SuccessThreshold,
		log:              log,
		states:           states,
	}
}

// Run probes immediately and then on each interval until ctx is cancelled.
func (c *Checker) Run(ctx context.Context) {
	c.probeAll(ctx)
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.probeAll(ctx)
		}
	}
}

// Healthy reports whether the backend is currently in rotation.
func (c *Checker) Healthy(shard, backend string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	st := c.states[key(shard, backend)]
	return st != nil && st.up
}

// PrimariesHealthy reports whether every primary has passed its probe.
func (c *Checker) PrimariesHealthy() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	saw := false
	for _, t := range c.targets {
		if t.Replica {
			continue
		}
		saw = true
		if !c.states[key(t.Shard, t.Backend.Name)].up {
			return false
		}
	}
	return saw
}

// Snapshot copies the latest status of every target.
func (c *Checker) Snapshot() []Status {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]Status, 0, len(c.targets))
	for _, t := range c.targets {
		st := c.states[key(t.Shard, t.Backend.Name)]
		out = append(out, Status{
			Shard:    t.Shard,
			Backend:  t.Backend.Name,
			Replica:  t.Replica,
			Up:       st.up,
			Lag:      st.lag,
			LagKnown: st.lagKnown,
			LastErr:  st.lastErr,
		})
	}
	return out
}

func (c *Checker) probeAll(ctx context.Context) {
	var wg sync.WaitGroup
	for _, t := range c.targets {
		wg.Add(1)
		go func(t Target) {
			defer wg.Done()
			pctx, cancel := context.WithTimeout(ctx, c.timeout)
			defer cancel()
			sample, err := c.prober(pctx, t.Backend, t.Replica)
			if ctx.Err() != nil {
				return
			}
			c.observe(t, sample, err)
		}(t)
	}
	wg.Wait()
}

func (c *Checker) observe(t Target, sample Sample, err error) {
	ok := err == nil
	reason := ""
	if err != nil {
		reason = err.Error()
		ok = false
	} else if t.Replica {
		switch {
		case !sample.ReplicationRunning:
			ok = false
			reason = "replication is not running"
		case !sample.LagKnown:
			ok = false
			reason = "replication lag is unknown"
		case sample.Lag > c.lagThreshold:
			ok = false
			reason = "replication lag exceeds threshold"
		}
	}
	id := key(t.Shard, t.Backend.Name)
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.states[id]
	if sample.LagKnown {
		st.lag = sample.Lag
		st.lagKnown = true
	}
	if ok {
		st.fails = 0
		st.oks++
		st.lastErr = ""
		if !st.up && st.oks >= c.successThreshold {
			st.up = true
			c.log.Info("backend entered rotation", "shard", t.Shard, "backend", t.Backend.Name, "replica", t.Replica)
		}
		return
	}
	st.oks = 0
	st.fails++
	st.lastErr = reason
	if st.up && st.fails >= c.failureThreshold {
		st.up = false
		c.log.Warn("backend left rotation", "shard", t.Shard, "backend", t.Backend.Name, "replica", t.Replica, "reason", reason)
	}
}

func key(shard, backend string) string {
	return shard + "/" + backend
}
