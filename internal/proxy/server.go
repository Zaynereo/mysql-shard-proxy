// Package proxy is a MySQL-protocol proxy that hash-shards queries and splits reads.
package proxy

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"log/slog"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Zaynereo/mysql-shard-proxy/internal/backend"
	"github.com/Zaynereo/mysql-shard-proxy/internal/config"
	"github.com/Zaynereo/mysql-shard-proxy/internal/health"
	"github.com/Zaynereo/mysql-shard-proxy/internal/metrics"
	"github.com/Zaynereo/mysql-shard-proxy/internal/pool"
	"github.com/Zaynereo/mysql-shard-proxy/internal/route"
)

// App is a running proxy.
type App struct {
	cfg     *config.Config
	log     *slog.Logger
	metrics *metrics.Metrics
	health  *health.Checker
	mysql   *server.Server
	cred    *server.InMemoryProvider

	pools    []*backendPool
	primary  []*backendPool
	replicas [][]*backendPool
	rr       []atomic.Uint64

	ln         net.Listener
	metricsLn  net.Listener
	http       *http.Server
	healthStop context.CancelFunc

	mu      sync.Mutex
	active  map[net.Conn]struct{}
	wg      sync.WaitGroup
	closing atomic.Bool
	ready   atomic.Bool
}

type backendPool struct {
	shard    string
	shardIdx int
	name     string
	role     string
	backend  config.Backend
	pool     *pool.Pool
}

// New builds pools, the health checker, and metrics for cfg.
func New(cfg *config.Config, log *slog.Logger) (*App, error) {
	if cfg == nil {
		return nil, errors.New("config is nil")
	}
	if log == nil {
		log = slog.Default()
	}
	app := &App{
		cfg:      cfg,
		log:      log,
		active:   map[net.Conn]struct{}{},
		primary:  make([]*backendPool, len(cfg.Shards)),
		replicas: make([][]*backendPool, len(cfg.Shards)),
		rr:       make([]atomic.Uint64, len(cfg.Shards)),
	}
	reg := prometheus.NewRegistry()
	app.metrics = metrics.New(reg, app.poolSnaps, app.healthSnaps)
	targets := make([]health.Target, 0, len(cfg.Shards)*2)
	for i, shard := range cfg.Shards {
		primary := app.addPool(shard.Name, i, shard.Primary, route.Primary)
		app.primary[i] = primary
		targets = append(targets, health.Target{Shard: shard.Name, Backend: shard.Primary, Replica: false})
		for _, replica := range shard.Replicas {
			app.replicas[i] = append(app.replicas[i], app.addPool(shard.Name, i, replica, route.Replica))
			targets = append(targets, health.Target{Shard: shard.Name, Backend: replica, Replica: true})
		}
	}
	app.health = health.New(targets, func(ctx context.Context, b config.Backend, replica bool) (health.Sample, error) {
		return health.ProbeMySQL(ctx, b, replica, cfg.Health.Timeout.Duration)
	}, cfg.Health, log)
	cred := server.NewInMemoryProvider()
	for _, user := range cfg.Users {
		cred.AddUser(user.Name, user.Password)
	}
	app.cred = cred
	app.mysql = server.NewServer(cfg.ServerVersion, mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, nil, nil)
	for _, shard := range cfg.Shards {
		log.Info("shard configured", "shard", shard.Name, "primary", shard.Primary.Addr, "replicas", len(shard.Replicas))
	}
	return app, nil
}

func (a *App) addPool(shard string, idx int, b config.Backend, role string) *backendPool {
	bp := &backendPool{shard: shard, shardIdx: idx, name: b.Name, role: role, backend: b}
	bp.pool = pool.New(pool.Config{
		MaxOpen:     a.cfg.Pool.MaxOpen,
		MaxIdle:     a.cfg.Pool.MaxIdle,
		MaxLifetime: a.cfg.Pool.ConnMaxLifetime.Duration,
		IdleTimeout: a.cfg.Pool.IdleTimeout.Duration,
	}, func(ctx context.Context) (pool.Conn, error) {
		conn, err := backend.Open(ctx, b, a.cfg.Pool.ReadTimeout.Duration, a.cfg.Pool.WriteTimeout.Duration, a.cfg.Pool.ConnectTimeout.Duration)
		if err != nil {
			return nil, err
		}
		return &tracked{Conn: conn}, nil
	})
	a.pools = append(a.pools, bp)
	return bp
}

// Serve starts the MySQL listener, metrics server, and health checker.
func (a *App) Serve(ctx context.Context) error {
	ln, err := net.Listen("tcp", a.cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen %s: %w", a.cfg.Listen, err)
	}
	mln, err := net.Listen("tcp", a.cfg.MetricsListen)
	if err != nil {
		_ = ln.Close()
		return fmt.Errorf("listen %s: %w", a.cfg.MetricsListen, err)
	}
	a.ln = ln
	a.metricsLn = mln
	healthCtx, stop := context.WithCancel(ctx)
	a.healthStop = stop
	go a.health.Run(healthCtx)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(a.metrics.Registry(), promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", a.handleHealth)
	mux.HandleFunc("/readyz", a.handleReady)
	a.http = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := a.http.Serve(mln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.log.Error("metrics server stopped", "err", err)
		}
	}()
	a.ready.Store(true)
	a.log.Info("proxy listening", "mysql", ln.Addr().String(), "metrics", mln.Addr().String())
	go a.accept()
	return nil
}

// Addr is the MySQL listen address.
func (a *App) Addr() string {
	if a.ln == nil {
		return ""
	}
	return a.ln.Addr().String()
}

// MetricsAddr is the HTTP listen address.
func (a *App) MetricsAddr() string {
	if a.metricsLn == nil {
		return ""
	}
	return a.metricsLn.Addr().String()
}

// Run serves until ctx is cancelled by SIGINT or SIGTERM, then shuts down.
func (a *App) Run(ctx context.Context) error {
	ctx, stop := signalContext(ctx)
	defer stop()
	if err := a.Serve(ctx); err != nil {
		return err
	}
	<-ctx.Done()
	a.log.Info("shutdown requested")
	sctx, cancel := context.WithTimeout(context.Background(), a.cfg.ShutdownTimeout.Duration)
	defer cancel()
	return a.Shutdown(sctx)
}

// Shutdown stops listeners, drains sessions, and closes pools.
func (a *App) Shutdown(ctx context.Context) error {
	a.closing.Store(true)
	a.ready.Store(false)
	if a.ln != nil {
		_ = a.ln.Close()
	}
	if a.http != nil {
		_ = a.http.Shutdown(ctx)
	}
	if a.healthStop != nil {
		a.healthStop()
	}
	done := make(chan struct{})
	go func() {
		a.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		a.mu.Lock()
		for conn := range a.active {
			_ = conn.Close()
		}
		a.mu.Unlock()
		<-done
	}
	for _, bp := range a.pools {
		bp.pool.Close()
	}
	a.log.Info("shutdown complete")
	return nil
}

func (a *App) accept() {
	for {
		conn, err := a.ln.Accept()
		if err != nil {
			if a.closing.Load() {
				return
			}
			a.log.Error("accept failed", "err", err)
			return
		}
		if a.closing.Load() {
			_ = conn.Close()
			return
		}
		a.wg.Add(1)
		a.mu.Lock()
		a.active[conn] = struct{}{}
		a.mu.Unlock()
		go a.serve(conn)
	}
}

func (a *App) serve(conn net.Conn) {
	defer a.wg.Done()
	defer func() {
		a.mu.Lock()
		delete(a.active, conn)
		a.mu.Unlock()
		_ = conn.Close()
	}()
	defer func() {
		if recovered := recover(); recovered != nil {
			a.log.Error("session panic", "err", recovered)
		}
	}()
	sess := newSession(a)
	defer sess.Close()
	sc, err := a.mysql.NewCustomizedConn(conn, a.cred, sess)
	if err != nil {
		a.log.Debug("client handshake failed", "err", err)
		return
	}
	for {
		if err := sc.HandleCommand(); err != nil {
			return
		}
	}
}

func (a *App) handleHealth(w http.ResponseWriter, _ *http.Request) {
	if a.closing.Load() {
		http.Error(w, "shutting down", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (a *App) handleReady(w http.ResponseWriter, _ *http.Request) {
	if !a.ready.Load() || a.closing.Load() || !a.health.PrimariesHealthy() {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready\n"))
}

func (a *App) poolSnaps() []metrics.PoolSnap {
	out := make([]metrics.PoolSnap, 0, len(a.pools))
	for _, bp := range a.pools {
		stats := bp.pool.Stats()
		out = append(out, metrics.PoolSnap{
			Shard:   bp.shard,
			Backend: bp.name,
			Role:    bp.role,
			Open:    stats.Open,
			Idle:    stats.Idle,
			InUse:   stats.InUse,
		})
	}
	return out
}

func (a *App) healthSnaps() []metrics.HealthSnap {
	if a.health == nil {
		return nil
	}
	snaps := a.health.Snapshot()
	out := make([]metrics.HealthSnap, 0, len(snaps))
	for _, snap := range snaps {
		role := route.Primary
		if snap.Replica {
			role = route.Replica
		}
		out = append(out, metrics.HealthSnap{
			Shard:    snap.Shard,
			Backend:  snap.Backend,
			Role:     role,
			Up:       snap.Up,
			Lag:      snap.Lag.Seconds(),
			LagKnown: snap.LagKnown,
			Replica:  snap.Replica,
		})
	}
	return out
}
