package config

import (
	"strings"
	"testing"
	"time"
)

func TestParseDefaultsAndEnv(t *testing.T) {
	t.Setenv("PROXY_APP_PASSWORD", "client-secret")
	t.Setenv("MYSQL_APP_PASSWORD", "backend-secret")
	cfg, err := Parse([]byte(`
shard_key: user_id
sharded_tables: [users, orders]
users:
  - name: app
    password_env: PROXY_APP_PASSWORD
shards:
  - name: shard0
    primary:
      addr: 127.0.0.1:3306
      user: app
      password_env: MYSQL_APP_PASSWORD
      database: app
    replicas:
      - addr: 127.0.0.1:3307
        user: app
        password_env: MYSQL_APP_PASSWORD
        database: app
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != "0.0.0.0:3306" {
		t.Fatalf("listen default: %s", cfg.Listen)
	}
	if cfg.ReadYourWrites.Duration != 5*time.Second {
		t.Fatalf("ryw default: %s", cfg.ReadYourWrites.Duration)
	}
	if cfg.Users[0].Password != "client-secret" {
		t.Fatal("client password was not resolved")
	}
	if cfg.Shards[0].Primary.Password != "backend-secret" {
		t.Fatal("backend password was not resolved")
	}
	if cfg.Shards[0].Primary.Name != "shard0-primary" {
		t.Fatalf("primary name: %s", cfg.Shards[0].Primary.Name)
	}
	if !cfg.ShardedTable("Users") || cfg.ShardedTable("other") {
		t.Fatalf("table classification failed")
	}
	if !cfg.FallbackToPrimary() {
		t.Fatal("expected fallback default")
	}
}

func TestParseRejectsMissingEnv(t *testing.T) {
	_, err := Parse([]byte(`
shard_key: user_id
users:
  - name: app
    password_env: MISSING_PROXY_PASSWORD_FOR_TEST
shards:
  - name: shard0
    primary:
      addr: 127.0.0.1:1
      user: app
      password: x
`))
	if err == nil || !strings.Contains(err.Error(), "MISSING_PROXY_PASSWORD_FOR_TEST") {
		t.Fatalf("got %v", err)
	}
}

func TestParseRejectsDuplicateShard(t *testing.T) {
	_, err := Parse([]byte(`
shard_key: id
users:
  - name: app
    password: secret
shards:
  - name: shard0
    primary: {name: a, addr: 127.0.0.1:1, user: u}
  - name: shard0
    primary: {name: b, addr: 127.0.0.1:2, user: u}
`))
	if err == nil || !strings.Contains(err.Error(), "duplicate shard") {
		t.Fatalf("got %v", err)
	}
}

func TestParseKnownField(t *testing.T) {
	_, err := Parse([]byte(`
shard_key: id
users:
  - name: app
    password: secret
not_a_field: true
shards:
  - name: shard0
    primary: {name: a, addr: 127.0.0.1:1, user: u}
`))
	if err == nil {
		t.Fatal("expected unknown field error")
	}
}

func TestReadYourWritesCanBeDisabled(t *testing.T) {
	cfg, err := Parse([]byte(`
shard_key: id
read_your_writes_window: 0s
users:
  - name: app
    password: secret
shards:
  - name: shard0
    primary: {name: a, addr: 127.0.0.1:1, user: u}
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ReadYourWrites.Duration != 0 {
		t.Fatalf("window = %s", cfg.ReadYourWrites.Duration)
	}
}

func TestExampleConfigLoads(t *testing.T) {
	t.Setenv("PROXY_APP_PASSWORD", "localdev")
	t.Setenv("MYSQL_APP_PASSWORD", "localdev")
	if _, err := Load("../../config.example.yaml"); err != nil {
		t.Fatal(err)
	}
	if _, err := Load("../../deploy/config.yaml"); err != nil {
		t.Fatal(err)
	}
}
