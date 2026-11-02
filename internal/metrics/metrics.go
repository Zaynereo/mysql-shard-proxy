// Package metrics exposes Prometheus collectors for the proxy.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// PoolSnap is one backend pool at scrape time.
type PoolSnap struct {
	Shard   string
	Backend string
	Role    string
	Open    int
	Idle    int
	InUse   int
}

// HealthSnap is one backend's health at scrape time.
type HealthSnap struct {
	Shard    string
	Backend  string
	Role     string
	Up       bool
	Lag      float64
	LagKnown bool
	Replica  bool
}

// Metrics records query traffic and scrapes pool and health gauges.
type Metrics struct {
	reg      *prometheus.Registry
	queries  *prometheus.CounterVec
	latency  *prometheus.HistogramVec
	inflight prometheus.Gauge
	fallback *prometheus.CounterVec
	poolDesc *prometheus.Desc
	upDesc   *prometheus.Desc
	lagDesc  *prometheus.Desc
	pools    func() []PoolSnap
	health   func() []HealthSnap
}

// New registers metrics on reg.
func New(reg *prometheus.Registry, pools func() []PoolSnap, health func() []HealthSnap) *Metrics {
	m := &Metrics{
		reg:    reg,
		pools:  pools,
		health: health,
		queries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mysql_shard_proxy_queries_total",
			Help: "Queries executed by shard, backend role, statement class, and result.",
		}, []string{"shard", "role", "op", "result"}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "mysql_shard_proxy_query_duration_seconds",
			Help:    "Backend query latency by shard and role.",
			Buckets: prometheus.DefBuckets,
		}, []string{"shard", "role"}),
		inflight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mysql_shard_proxy_inflight_queries",
			Help: "Queries currently running on a backend.",
		}),
		fallback: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mysql_shard_proxy_read_fallbacks_total",
			Help: "Reads served by a primary because no replica was in rotation.",
		}, []string{"shard"}),
		poolDesc: prometheus.NewDesc(
			"mysql_shard_proxy_pool_connections",
			"Backend pool connections by state.",
			[]string{"shard", "backend", "role", "state"},
			nil,
		),
		upDesc: prometheus.NewDesc(
			"mysql_shard_proxy_backend_up",
			"1 when the backend is in rotation.",
			[]string{"shard", "backend", "role"},
			nil,
		),
		lagDesc: prometheus.NewDesc(
			"mysql_shard_proxy_replica_lag_seconds",
			"Replica replication lag in seconds.",
			[]string{"shard", "replica"},
			nil,
		),
	}
	reg.MustRegister(m.queries, m.latency, m.inflight, m.fallback, m)
	return m
}

// Registry returns the registry served on /metrics.
func (m *Metrics) Registry() *prometheus.Registry {
	return m.reg
}

// Observe records one backend execution.
func (m *Metrics) Observe(shard, role, op string, err error, d time.Duration) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	if shard == "" {
		shard = "none"
	}
	if role == "" {
		role = "none"
	}
	if op == "" {
		op = "other"
	}
	m.queries.WithLabelValues(shard, role, op, result).Inc()
	if shard != "none" && role != "none" {
		m.latency.WithLabelValues(shard, role).Observe(d.Seconds())
	}
}

// Fallback records a read that used the primary.
func (m *Metrics) Fallback(shard string) {
	m.fallback.WithLabelValues(shard).Inc()
}

// Begin starts an in-flight query.
func (m *Metrics) Begin() { m.inflight.Inc() }

// End finishes an in-flight query.
func (m *Metrics) End() { m.inflight.Dec() }

// Describe implements prometheus.Collector.
func (m *Metrics) Describe(ch chan<- *prometheus.Desc) {
	ch <- m.poolDesc
	ch <- m.upDesc
	ch <- m.lagDesc
}

// Collect implements prometheus.Collector for pool and health gauges.
func (m *Metrics) Collect(ch chan<- prometheus.Metric) {
	if m.pools != nil {
		for _, p := range m.pools() {
			ch <- prometheus.MustNewConstMetric(m.poolDesc, prometheus.GaugeValue, float64(p.Open), p.Shard, p.Backend, p.Role, "open")
			ch <- prometheus.MustNewConstMetric(m.poolDesc, prometheus.GaugeValue, float64(p.Idle), p.Shard, p.Backend, p.Role, "idle")
			ch <- prometheus.MustNewConstMetric(m.poolDesc, prometheus.GaugeValue, float64(p.InUse), p.Shard, p.Backend, p.Role, "in_use")
		}
	}
	if m.health == nil {
		return
	}
	for _, h := range m.health() {
		up := 0.0
		if h.Up {
			up = 1
		}
		ch <- prometheus.MustNewConstMetric(m.upDesc, prometheus.GaugeValue, up, h.Shard, h.Backend, h.Role)
		if h.Replica && h.LagKnown {
			ch <- prometheus.MustNewConstMetric(m.lagDesc, prometheus.GaugeValue, h.Lag, h.Shard, h.Backend)
		}
	}
}
