package proxy

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"

	"github.com/Zaynereo/mysql-shard-proxy/internal/config"
	"github.com/Zaynereo/mysql-shard-proxy/internal/logging"
	"github.com/Zaynereo/mysql-shard-proxy/internal/shard"
	"github.com/Zaynereo/mysql-shard-proxy/internal/testutil/mysqlfake"
)

type cluster struct {
	app   *App
	nodes map[string]*mysqlfake.Server
}

func startCluster(t *testing.T, ryw time.Duration) *cluster {
	t.Helper()
	nodes := map[string]*mysqlfake.Server{}
	start := func(name string) *mysqlfake.Server {
		t.Helper()
		srv, err := mysqlfake.Start("backend", "backend-secret")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(srv.Close)
		nodes[name] = srv
		return srv
	}
	backend := func(name string) config.Backend {
		return config.Backend{
			Name: name, Addr: nodes[name].Addr, User: "backend", Password: "backend-secret", Database: "app",
		}
	}
	for _, name := range []string{
		"s0p", "s0r0", "s0r1", "s1p", "s1r0", "s1r1",
	} {
		start(name)
	}
	fallback := true
	cfg := &config.Config{
		Listen:           "127.0.0.1:0",
		MetricsListen:    "127.0.0.1:0",
		ServerVersion:    "8.0.36-shard-proxy",
		LogLevel:         "error",
		ShutdownTimeout:  config.Duration{Duration: 2 * time.Second, Set: true},
		ShardKey:         "user_id",
		ShardedTables:    []string{"users", "orders"},
		ReadYourWrites:   config.Duration{Duration: ryw, Set: true},
		FallbackPrimary:  &fallback,
		AllowMultiRead:   true,
		AllowScatterRead: false,
		Users:            []config.User{{Name: "app", Password: "client-secret"}},
		Pool: config.PoolConfig{
			MaxOpen:         8,
			MaxIdle:         4,
			ConnectTimeout:  config.Duration{Duration: time.Second, Set: true},
			AcquireTimeout:  config.Duration{Duration: time.Second, Set: true},
			ConnMaxLifetime: config.Duration{Duration: time.Minute, Set: true},
			IdleTimeout:     config.Duration{Duration: time.Minute, Set: true},
		},
		Health: config.HealthConfig{
			Interval:         config.Duration{Duration: 20 * time.Millisecond, Set: true},
			Timeout:          config.Duration{Duration: 500 * time.Millisecond, Set: true},
			LagThreshold:     config.Duration{Duration: time.Second, Set: true},
			FailureThreshold: 2,
			SuccessThreshold: 1,
		},
		Shards: []config.Shard{
			{Name: "shard0", Primary: backend("s0p"), Replicas: []config.Backend{backend("s0r0"), backend("s0r1")}},
			{Name: "shard1", Primary: backend("s1p"), Replicas: []config.Backend{backend("s1r0"), backend("s1r1")}},
		},
	}
	app, err := New(cfg, logging.NewWriter(io.Discard, "error"))
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Serve(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = app.Shutdown(ctx)
	})
	c := &cluster{app: app, nodes: nodes}
	c.waitReady(t)
	return c
}

func (c *cluster) waitReady(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if c.app.health.PrimariesHealthy() &&
			c.app.health.Healthy("shard0", "s0r0") &&
			c.app.health.Healthy("shard0", "s0r1") &&
			c.app.health.Healthy("shard1", "s1r0") &&
			c.app.health.Healthy("shard1", "s1r1") {
			resp, err := http.Get("http://" + c.app.MetricsAddr() + "/readyz")
			if err == nil {
				_ = resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					return
				}
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatal("cluster did not become ready")
}

func (c *cluster) waitReplica(t *testing.T, shard, name string, want bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if c.app.health.Healthy(shard, name) == want {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("replica %s/%s healthy=%v, want %v", shard, name, c.app.health.Healthy(shard, name), want)
}

func (c *cluster) count(name, fragment string) int {
	n := 0
	for _, q := range c.nodes[name].Queries() {
		if strings.Contains(strings.ToLower(q), strings.ToLower(fragment)) {
			n++
		}
	}
	return n
}

func connect(t *testing.T, app *App) *client.Conn {
	t.Helper()
	conn, err := client.Connect(app.Addr(), "app", "client-secret", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestProxyRoutesReadsWritesAndTransactions(t *testing.T) {
	c := startCluster(t, 200*time.Millisecond)
	if _, err := client.Connect(c.app.Addr(), "app", "nope", ""); err == nil {
		t.Fatal("expected authentication failure")
	}

	key := "42"
	idx, err := shard.Index(key, 2)
	if err != nil {
		t.Fatal(err)
	}
	other := otherKey(t, idx)
	primary := []string{"s0p", "s1p"}[idx]
	replicas := [][]string{{"s0r0", "s0r1"}, {"s1r0", "s1r1"}}[idx]
	otherPrimary := []string{"s0p", "s1p"}[1-idx]

	conn := connect(t, c.app)
	selectSQL := "SELECT name FROM users WHERE user_id = " + key
	before := c.count(replicas[0], selectSQL) + c.count(replicas[1], selectSQL)
	if _, err := conn.Execute(selectSQL); err != nil {
		t.Fatal(err)
	}
	after := c.count(replicas[0], selectSQL) + c.count(replicas[1], selectSQL)
	if after != before+1 {
		t.Fatalf("select did not hit a replica (before %d after %d)", before, after)
	}
	if c.count(primary, selectSQL) != 0 {
		t.Fatal("first select hit the primary")
	}

	insert := "INSERT INTO users (id, user_id, name) VALUES (1, " + key + ", 'ada')"
	if _, err := conn.Execute(insert); err != nil {
		t.Fatal(err)
	}
	if c.count(primary, insert) != 1 {
		t.Fatal("insert did not hit the owning primary")
	}
	if c.count(otherPrimary, insert) != 0 {
		t.Fatal("insert hit the other primary")
	}
	if _, err := conn.Execute(selectSQL); err != nil {
		t.Fatal(err)
	}
	if c.count(primary, selectSQL) != 1 {
		t.Fatal("read-your-writes select did not stay on the primary")
	}
	time.Sleep(350 * time.Millisecond)
	if _, err := conn.Execute(selectSQL); err != nil {
		t.Fatal(err)
	}
	if c.count(replicas[0], selectSQL)+c.count(replicas[1], selectSQL) != 2 {
		t.Fatal("select after the read-your-writes window did not return to a replica")
	}

	tx := connect(t, c.app)
	if _, err := tx.Execute("BEGIN"); err != nil {
		t.Fatal(err)
	}
	txInsert := "INSERT INTO users (id, user_id, name) VALUES (2, " + key + ", 'bea')"
	if _, err := tx.Execute(txInsert); err != nil {
		t.Fatal(err)
	}
	txSelect := "SELECT name FROM users WHERE user_id = " + key + " AND name = 'bea'"
	if _, err := tx.Execute(txSelect); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Execute("COMMIT"); err != nil {
		t.Fatal(err)
	}
	if c.count(primary, "BEGIN") < 1 || c.count(primary, txInsert) != 1 || c.count(primary, txSelect) != 1 || c.count(primary, "COMMIT") < 1 {
		t.Fatalf("transaction was not pinned to %s: %#v", primary, c.nodes[primary].Queries())
	}
	if c.count(replicas[0], txSelect)+c.count(replicas[1], txSelect) != 0 {
		t.Fatal("transactional select hit a replica")
	}

	cross := connect(t, c.app)
	if _, err := cross.Execute("BEGIN"); err != nil {
		t.Fatal(err)
	}
	if _, err := cross.Execute(insert); err != nil {
		t.Fatal(err)
	}
	_, err = cross.Execute("INSERT INTO users (id, user_id, name) VALUES (3, " + other + ", 'cy')")
	if err == nil || !strings.Contains(err.Error(), "cross-shard") {
		t.Fatalf("expected cross-shard error, got %v", err)
	}
	_, _ = cross.Execute("ROLLBACK")

	if _, err := connect(t, c.app).Execute("SELECT * FROM users"); err == nil || !strings.Contains(err.Error(), "shard key") {
		t.Fatalf("expected missing shard key, got %v", err)
	}
	if _, err := connect(t, c.app).Execute("SELECT * FROM users u JOIN orders o ON o.user_id = u.user_id"); err == nil || !strings.Contains(strings.ToLower(err.Error()), "join") {
		t.Fatalf("expected join rejection, got %v", err)
	}

	prep := connect(t, c.app)
	stmt, err := prep.Prepare("SELECT name FROM users WHERE user_id = ?")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stmt.Close() }()
	if _, err := stmt.Execute(int64(42)); err != nil {
		t.Fatal(err)
	}
	if c.count(replicas[0], "EXECUTE SELECT name FROM users WHERE user_id = ?")+c.count(replicas[1], "EXECUTE SELECT name FROM users WHERE user_id = ?")+c.count(primary, "EXECUTE SELECT name FROM users WHERE user_id = ?") != 1 {
		t.Fatal("prepared select was not executed on the owning shard")
	}

	session := connect(t, c.app)
	if _, err := session.Execute("SET NAMES utf8mb4"); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Execute("USE app"); err != nil {
		t.Fatal(err)
	}
	marked := "SELECT name FROM users WHERE user_id = " + key + " AND name = 'session'"
	if _, err := session.Execute(marked); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, name := range append(append([]string{}, replicas...), primary) {
		queries := c.nodes[name].Queries()
		for i, q := range queries {
			if strings.Contains(q, "name = 'session'") {
				prev := strings.Join(queries[:i], "\n")
				if strings.Contains(prev, "SET NAMES utf8mb4") && strings.Contains(strings.ToUpper(prev), "USE APP") {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatal("session SET NAMES and USE were not replayed before the query")
	}

	k1, k2 := splitKeys(t)
	multi := "SELECT name FROM users WHERE user_id IN (" + k1 + ", " + k2 + ")"
	if _, err := connect(t, c.app).Execute(multi); err != nil {
		t.Fatal(err)
	}
	if c.count("s0p", multi)+c.count("s0r0", multi)+c.count("s0r1", multi) == 0 ||
		c.count("s1p", multi)+c.count("s1r0", multi)+c.count("s1r1", multi) == 0 {
		t.Fatal("multi-shard read did not touch both shards")
	}
	if _, err := connect(t, c.app).Execute(multi + " ORDER BY name"); err == nil {
		t.Fatal("expected ordered multi-shard read to be rejected")
	}

	ddl := "CREATE TABLE users (id BIGINT, user_id BIGINT, name VARCHAR(32))"
	if _, err := connect(t, c.app).Execute(ddl); err != nil {
		t.Fatal(err)
	}
	if c.count("s0p", ddl) != 1 || c.count("s1p", ddl) != 1 {
		t.Fatal("DDL was not fanned out to both primaries")
	}
}

func TestReplicaHealthRemovesAndRestores(t *testing.T) {
	c := startCluster(t, 0)
	key := "42"
	idx, err := shard.Index(key, 2)
	if err != nil {
		t.Fatal(err)
	}
	shardName := []string{"shard0", "shard1"}[idx]
	primary := []string{"s0p", "s1p"}[idx]
	reps := [][]string{{"s0r0", "s0r1"}, {"s1r0", "s1r1"}}[idx]
	sql := "SELECT name FROM users WHERE user_id = " + key + " AND name = 'health'"

	c.nodes[reps[0]].SetReplica(true, 30*time.Second)
	c.nodes[reps[1]].SetReplica(true, 30*time.Second)
	c.waitReplica(t, shardName, reps[0], false)
	c.waitReplica(t, shardName, reps[1], false)
	if _, err := connect(t, c.app).Execute(sql); err != nil {
		t.Fatal(err)
	}
	if c.count(primary, sql) != 1 {
		t.Fatal("unhealthy replicas did not fall back to the primary")
	}

	c.nodes[reps[0]].SetReplica(true, 0)
	c.waitReplica(t, shardName, reps[0], true)
	before := c.count(reps[0], sql)
	if _, err := connect(t, c.app).Execute(sql); err != nil {
		t.Fatal(err)
	}
	if c.count(reps[0], sql) != before+1 {
		t.Fatal("restored replica did not receive the read")
	}
	if c.count(reps[1], sql) != 0 {
		t.Fatal("lagging replica received a read")
	}
}

func TestMetricsAndShutdown(t *testing.T) {
	c := startCluster(t, 0)
	if _, err := connect(t, c.app).Execute("SELECT name FROM users WHERE user_id = 7"); err != nil {
		t.Fatal(err)
	}
	resp, err := http.Get("http://" + c.app.MetricsAddr() + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, needle := range []string{
		"mysql_shard_proxy_queries_total",
		"mysql_shard_proxy_query_duration_seconds",
		"mysql_shard_proxy_pool_connections",
		"mysql_shard_proxy_backend_up",
		"mysql_shard_proxy_replica_lag_seconds",
	} {
		if !strings.Contains(text, needle) {
			t.Fatalf("metrics missing %s", needle)
		}
	}
	healthResp, err := http.Get("http://" + c.app.MetricsAddr() + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	_ = healthResp.Body.Close()
	if healthResp.StatusCode != http.StatusOK {
		t.Fatalf("healthz %d", healthResp.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.app.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Connect(c.app.Addr(), "app", "client-secret", ""); err == nil {
		t.Fatal("listener still accepted a connection after shutdown")
	}
}

func otherKey(t *testing.T, idx int) string {
	t.Helper()
	for i := 0; i < 100; i++ {
		key := string(rune('a' + i))
		got, err := shard.Index(key, 2)
		if err != nil {
			t.Fatal(err)
		}
		if got != idx {
			return "'" + key + "'"
		}
	}
	t.Fatal("no key on the other shard")
	return ""
}

func splitKeys(t *testing.T) (string, string) {
	t.Helper()
	var keys [2]string
	found := map[int]bool{}
	for i := 0; i < 100 && len(found) < 2; i++ {
		key := string(rune('a' + i))
		idx, err := shard.Index(key, 2)
		if err != nil {
			t.Fatal(err)
		}
		if !found[idx] {
			found[idx] = true
			keys[idx] = "'" + key + "'"
		}
	}
	if keys[0] == "" || keys[1] == "" {
		t.Fatal("could not find keys on both shards")
	}
	return keys[0], keys[1]
}
