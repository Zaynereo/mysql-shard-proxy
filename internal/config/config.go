// Package config loads and validates the proxy YAML configuration.
package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration is a YAML string parsed with time.ParseDuration.
// Set reports whether the field was present in the file.
type Duration struct {
	time.Duration
	Set bool
}

// UnmarshalYAML parses a duration string such as "5s".
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		var n int64
		if err2 := value.Decode(&n); err2 != nil {
			return fmt.Errorf("duration: %w", err)
		}
		d.Duration = time.Duration(n)
		d.Set = true
		return nil
	}
	parsed, err := time.ParseDuration(strings.TrimSpace(s))
	if err != nil {
		return fmt.Errorf("duration %q: %w", s, err)
	}
	d.Duration = parsed
	d.Set = true
	return nil
}

// Config is the proxy configuration.
type Config struct {
	Listen           string       `yaml:"listen"`
	MetricsListen    string       `yaml:"metrics_listen"`
	ServerVersion    string       `yaml:"server_version"`
	LogLevel         string       `yaml:"log_level"`
	ShutdownTimeout  Duration     `yaml:"shutdown_timeout"`
	ShardKey         string       `yaml:"shard_key"`
	ShardedTables    []string     `yaml:"sharded_tables"`
	ReadYourWrites   Duration     `yaml:"read_your_writes_window"`
	FallbackPrimary  *bool        `yaml:"fallback_to_primary"`
	AllowMultiRead   bool         `yaml:"allow_multi_shard_read"`
	AllowScatterRead bool         `yaml:"allow_scatter_read"`
	Users            []User       `yaml:"users"`
	Pool             PoolConfig   `yaml:"pool"`
	Health           HealthConfig `yaml:"health"`
	Shards           []Shard      `yaml:"shards"`
}

// User is a client account accepted by the proxy.
type User struct {
	Name        string `yaml:"name"`
	Password    string `yaml:"password"`
	PasswordEnv string `yaml:"password_env"`
}

// PoolConfig limits backend connections.
type PoolConfig struct {
	MaxOpen         int      `yaml:"max_open"`
	MaxIdle         int      `yaml:"max_idle"`
	ConnectTimeout  Duration `yaml:"connect_timeout"`
	AcquireTimeout  Duration `yaml:"acquire_timeout"`
	ConnMaxLifetime Duration `yaml:"conn_max_lifetime"`
	IdleTimeout     Duration `yaml:"idle_timeout"`
	ReadTimeout     Duration `yaml:"read_timeout"`
	WriteTimeout    Duration `yaml:"write_timeout"`
}

// HealthConfig controls replica and primary probes.
type HealthConfig struct {
	Interval         Duration `yaml:"interval"`
	Timeout          Duration `yaml:"timeout"`
	LagThreshold     Duration `yaml:"lag_threshold"`
	FailureThreshold int      `yaml:"failure_threshold"`
	SuccessThreshold int      `yaml:"success_threshold"`
}

// Shard is one hash partition: a primary and zero or more replicas.
type Shard struct {
	Name     string    `yaml:"name"`
	Primary  Backend   `yaml:"primary"`
	Replicas []Backend `yaml:"replicas"`
}

// Backend is a MySQL endpoint.
type Backend struct {
	Name        string `yaml:"name"`
	Addr        string `yaml:"addr"`
	User        string `yaml:"user"`
	Password    string `yaml:"password"`
	PasswordEnv string `yaml:"password_env"`
	Database    string `yaml:"database"`
}

// Load reads a YAML config file and validates it.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return Parse(b)
}

// Parse decodes YAML and validates it.
func Parse(b []byte) (*Config, error) {
	var cfg Config
	dec := yaml.NewDecoder(strings.NewReader(string(b)))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.resolveSecrets(); err != nil {
		return nil, err
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) resolveSecrets() error {
	for i := range c.Users {
		if err := resolvePassword("user "+c.Users[i].Name, &c.Users[i].Password, c.Users[i].PasswordEnv); err != nil {
			return err
		}
	}
	for i := range c.Shards {
		if err := resolvePassword(
			"shard "+c.Shards[i].Name+" primary",
			&c.Shards[i].Primary.Password,
			c.Shards[i].Primary.PasswordEnv,
		); err != nil {
			return err
		}
		for j := range c.Shards[i].Replicas {
			if err := resolvePassword(
				"shard "+c.Shards[i].Name+" replica "+c.Shards[i].Replicas[j].Name,
				&c.Shards[i].Replicas[j].Password,
				c.Shards[i].Replicas[j].PasswordEnv,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func resolvePassword(who string, password *string, envName string) error {
	if envName == "" {
		return nil
	}
	v, ok := os.LookupEnv(envName)
	if !ok {
		return fmt.Errorf("%s: environment variable %s is not set", who, envName)
	}
	*password = v
	return nil
}

func (c *Config) applyDefaults() {
	if c.Listen == "" {
		c.Listen = "0.0.0.0:3306"
	}
	if c.MetricsListen == "" {
		c.MetricsListen = "0.0.0.0:9090"
	}
	if c.ServerVersion == "" {
		c.ServerVersion = "8.0.36-shard-proxy"
	}
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
	if !c.ShutdownTimeout.Set {
		c.ShutdownTimeout = Duration{Duration: 20 * time.Second, Set: true}
	}
	if !c.ReadYourWrites.Set {
		c.ReadYourWrites = Duration{Duration: 5 * time.Second, Set: true}
	}
	if c.FallbackPrimary == nil {
		v := true
		c.FallbackPrimary = &v
	}
	if c.Pool.MaxOpen == 0 {
		c.Pool.MaxOpen = 16
	}
	if c.Pool.MaxIdle == 0 {
		c.Pool.MaxIdle = 4
	}
	if !c.Pool.ConnectTimeout.Set {
		c.Pool.ConnectTimeout = Duration{Duration: 3 * time.Second, Set: true}
	}
	if !c.Pool.AcquireTimeout.Set {
		c.Pool.AcquireTimeout = Duration{Duration: 2 * time.Second, Set: true}
	}
	if !c.Pool.ConnMaxLifetime.Set {
		c.Pool.ConnMaxLifetime = Duration{Duration: 30 * time.Minute, Set: true}
	}
	if !c.Pool.IdleTimeout.Set {
		c.Pool.IdleTimeout = Duration{Duration: 5 * time.Minute, Set: true}
	}
	if !c.Health.Interval.Set {
		c.Health.Interval = Duration{Duration: 2 * time.Second, Set: true}
	}
	if !c.Health.Timeout.Set {
		c.Health.Timeout = Duration{Duration: time.Second, Set: true}
	}
	if !c.Health.LagThreshold.Set {
		c.Health.LagThreshold = Duration{Duration: 5 * time.Second, Set: true}
	}
	if c.Health.FailureThreshold == 0 {
		c.Health.FailureThreshold = 2
	}
	if c.Health.SuccessThreshold == 0 {
		c.Health.SuccessThreshold = 1
	}
	for i := range c.Shards {
		if c.Shards[i].Primary.Name == "" {
			c.Shards[i].Primary.Name = c.Shards[i].Name + "-primary"
		}
		for j := range c.Shards[i].Replicas {
			if c.Shards[i].Replicas[j].Name == "" {
				c.Shards[i].Replicas[j].Name = fmt.Sprintf("%s-replica-%d", c.Shards[i].Name, j)
			}
		}
	}
}

// Validate returns an error when the configuration cannot be served.
func (c *Config) Validate() error {
	if strings.TrimSpace(c.ShardKey) == "" {
		return fmt.Errorf("shard_key is required")
	}
	if strings.ContainsAny(c.ShardKey, " \t`\"'") {
		return fmt.Errorf("shard_key must be a bare column name")
	}
	if len(c.Shards) == 0 {
		return fmt.Errorf("at least one shard is required")
	}
	if len(c.Users) == 0 {
		return fmt.Errorf("at least one client user is required")
	}
	if c.ReadYourWrites.Duration < 0 {
		return fmt.Errorf("read_your_writes_window must be >= 0")
	}
	if c.ShutdownTimeout.Duration <= 0 {
		return fmt.Errorf("shutdown_timeout must be > 0")
	}
	if c.Pool.MaxOpen < 1 {
		return fmt.Errorf("pool.max_open must be >= 1")
	}
	if c.Pool.MaxIdle < 0 || c.Pool.MaxIdle > c.Pool.MaxOpen {
		return fmt.Errorf("pool.max_idle must be between 0 and pool.max_open")
	}
	if c.Pool.ConnectTimeout.Duration <= 0 || c.Pool.AcquireTimeout.Duration <= 0 {
		return fmt.Errorf("pool connect_timeout and acquire_timeout must be > 0")
	}
	if c.Pool.ConnMaxLifetime.Duration <= 0 || c.Pool.IdleTimeout.Duration <= 0 {
		return fmt.Errorf("pool conn_max_lifetime and idle_timeout must be > 0")
	}
	if c.Health.Interval.Duration <= 0 || c.Health.Timeout.Duration <= 0 {
		return fmt.Errorf("health interval and timeout must be > 0")
	}
	if c.Health.LagThreshold.Duration < 0 {
		return fmt.Errorf("health.lag_threshold must be >= 0")
	}
	if c.Health.FailureThreshold < 1 || c.Health.SuccessThreshold < 1 {
		return fmt.Errorf("health failure_threshold and success_threshold must be >= 1")
	}
	users := map[string]struct{}{}
	for _, u := range c.Users {
		if u.Name == "" {
			return fmt.Errorf("client user name is required")
		}
		if _, ok := users[u.Name]; ok {
			return fmt.Errorf("duplicate client user %q", u.Name)
		}
		users[u.Name] = struct{}{}
		if u.Password == "" {
			return fmt.Errorf("client user %q has an empty password", u.Name)
		}
	}
	names := map[string]struct{}{}
	backends := map[string]struct{}{}
	for _, sh := range c.Shards {
		if sh.Name == "" {
			return fmt.Errorf("shard name is required")
		}
		if _, ok := names[sh.Name]; ok {
			return fmt.Errorf("duplicate shard name %q", sh.Name)
		}
		names[sh.Name] = struct{}{}
		if err := validateBackend(sh.Name, "primary", sh.Primary, backends); err != nil {
			return err
		}
		for _, r := range sh.Replicas {
			if err := validateBackend(sh.Name, "replica", r, backends); err != nil {
				return err
			}
		}
	}
	seenTable := map[string]struct{}{}
	for _, t := range c.ShardedTables {
		key := strings.ToLower(t)
		if key == "" {
			return fmt.Errorf("sharded table name is empty")
		}
		if _, ok := seenTable[key]; ok {
			return fmt.Errorf("duplicate sharded table %q", t)
		}
		seenTable[key] = struct{}{}
	}
	return nil
}

func validateBackend(shard, role string, b Backend, seen map[string]struct{}) error {
	if b.Name == "" || b.Addr == "" || b.User == "" {
		return fmt.Errorf("shard %s %s requires name, addr, and user", shard, role)
	}
	id := shard + "/" + b.Name
	if _, ok := seen[id]; ok {
		return fmt.Errorf("duplicate backend %q", id)
	}
	seen[id] = struct{}{}
	return nil
}

// FallbackToPrimary reports whether reads may use the primary when no replica is healthy.
func (c *Config) FallbackToPrimary() bool {
	return c.FallbackPrimary == nil || *c.FallbackPrimary
}

// ShardedTable reports whether table is routed by the shard key.
// An empty sharded_tables list means every table is sharded.
func (c *Config) ShardedTable(table string) bool {
	if len(c.ShardedTables) == 0 {
		return true
	}
	table = strings.ToLower(table)
	for _, t := range c.ShardedTables {
		if strings.ToLower(t) == table {
			return true
		}
	}
	return false
}
