// Package pool is a per-backend connection pool with limits and timeouts.
package pool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// ErrClosed is returned when the pool has been closed.
var ErrClosed = errors.New("connection pool closed")

// Conn is the subset of a MySQL client connection the proxy uses.
type Conn interface {
	Execute(query string, args ...any) (*mysql.Result, error)
	UseDB(name string) error
	Prepare(query string) (Stmt, error)
	FieldList(table, wildcard string) ([]*mysql.Field, error)
	Close() error
}

// Stmt is a server-side prepared statement.
type Stmt interface {
	Execute(args ...any) (*mysql.Result, error)
	Close() error
	ParamCount() int
	ColumnCount() int
}

// Factory opens one backend connection.
type Factory func(ctx context.Context) (Conn, error)

// Config controls pool capacity and deadlines.
type Config struct {
	MaxOpen     int
	MaxIdle     int
	MaxLifetime time.Duration
	IdleTimeout time.Duration
}

// Stats is a point-in-time snapshot of pool usage.
type Stats struct {
	Open    int
	Idle    int
	InUse   int
	Waiters int
}

// Pool borrows backend connections.
type Pool struct {
	cfg     Config
	factory Factory

	mu      sync.Mutex
	idle    []*idleConn
	open    int
	inUse   int
	waiters []chan struct{}
	closed  bool

	stop context.CancelFunc
}

type idleConn struct {
	conn      Conn
	created   time.Time
	idleSince time.Time
}

// Lease is a borrowed connection. Release returns it to the pool.
type Lease struct {
	pool     *Pool
	conn     Conn
	created  time.Time
	released bool
}

// Conn returns the borrowed connection.
func (l *Lease) Conn() Conn {
	if l == nil {
		return nil
	}
	return l.conn
}

// Release returns the connection to the pool.
// broken closes it instead of reusing it.
func (l *Lease) Release(broken bool) {
	if l == nil || l.released {
		return
	}
	l.released = true
	l.pool.put(l.conn, l.created, broken)
}

// New starts a pool. The caller must Close it.
func New(cfg Config, factory Factory) *Pool {
	if cfg.MaxOpen < 1 {
		cfg.MaxOpen = 1
	}
	if cfg.MaxIdle < 0 {
		cfg.MaxIdle = 0
	}
	if cfg.MaxIdle > cfg.MaxOpen {
		cfg.MaxIdle = cfg.MaxOpen
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pool{cfg: cfg, factory: factory, stop: cancel}
	go p.reapLoop(ctx)
	return p
}

// Get borrows a connection, waiting until ctx is done if the pool is at capacity.
func (p *Pool) Get(ctx context.Context) (*Lease, error) {
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, ErrClosed
		}
		if conn, created, ok := p.takeIdleLocked(time.Now()); ok {
			p.inUse++
			p.mu.Unlock()
			return &Lease{pool: p, conn: conn, created: created}, nil
		}
		if p.open < p.cfg.MaxOpen {
			p.open++
			p.mu.Unlock()
			conn, err := p.factory(ctx)
			if err != nil {
				p.mu.Lock()
				p.open--
				p.wakeOneLocked()
				p.mu.Unlock()
				return nil, err
			}
			p.mu.Lock()
			if p.closed {
				p.open--
				p.mu.Unlock()
				_ = conn.Close()
				return nil, ErrClosed
			}
			p.inUse++
			p.mu.Unlock()
			return &Lease{pool: p, conn: conn, created: time.Now()}, nil
		}
		ch := make(chan struct{}, 1)
		p.waiters = append(p.waiters, ch)
		p.mu.Unlock()

		select {
		case <-ctx.Done():
			p.mu.Lock()
			removed := p.removeWaiterLocked(ch)
			if !removed {
				p.wakeOneLocked()
			}
			p.mu.Unlock()
			return nil, fmt.Errorf("acquire connection: %w", ctx.Err())
		case <-ch:
		}
	}
}

// Stats returns current usage.
func (p *Pool) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return Stats{
		Open:    p.open,
		Idle:    len(p.idle),
		InUse:   p.inUse,
		Waiters: len(p.waiters),
	}
}

// Close stops the reaper, fails waiters, and closes idle connections.
// Borrowed connections close when they are released.
func (p *Pool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	idle := p.idle
	p.idle = nil
	p.open -= len(idle)
	waiters := p.waiters
	p.waiters = nil
	stop := p.stop
	p.mu.Unlock()
	if stop != nil {
		stop()
	}
	for _, ic := range idle {
		_ = ic.conn.Close()
	}
	for _, ch := range waiters {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (p *Pool) put(conn Conn, created time.Time, broken bool) {
	now := time.Now()
	p.mu.Lock()
	p.inUse--
	expired := p.cfg.MaxLifetime > 0 && now.Sub(created) >= p.cfg.MaxLifetime
	if broken || expired || p.closed || len(p.idle) >= p.cfg.MaxIdle {
		p.open--
		p.wakeOneLocked()
		p.mu.Unlock()
		_ = conn.Close()
		return
	}
	p.idle = append(p.idle, &idleConn{conn: conn, created: created, idleSince: now})
	p.wakeOneLocked()
	p.mu.Unlock()
}

func (p *Pool) takeIdleLocked(now time.Time) (Conn, time.Time, bool) {
	for len(p.idle) > 0 {
		ic := p.idle[0]
		p.idle = p.idle[1:]
		expired := p.cfg.MaxLifetime > 0 && now.Sub(ic.created) >= p.cfg.MaxLifetime
		stale := p.cfg.IdleTimeout > 0 && now.Sub(ic.idleSince) >= p.cfg.IdleTimeout
		if expired || stale {
			p.open--
			go func(c Conn) { _ = c.Close() }(ic.conn)
			continue
		}
		return ic.conn, ic.created, true
	}
	return nil, time.Time{}, false
}

func (p *Pool) wakeOneLocked() {
	if len(p.waiters) == 0 {
		return
	}
	ch := p.waiters[0]
	p.waiters = p.waiters[1:]
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (p *Pool) removeWaiterLocked(ch chan struct{}) bool {
	for i, w := range p.waiters {
		if w == ch {
			p.waiters = append(p.waiters[:i], p.waiters[i+1:]...)
			return true
		}
	}
	return false
}

func (p *Pool) reapLoop(ctx context.Context) {
	interval := p.cfg.IdleTimeout / 2
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	if interval > 30*time.Second {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.reap()
		}
	}
}

func (p *Pool) reap() {
	now := time.Now()
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	keep := p.idle[:0]
	var drop []Conn
	for _, ic := range p.idle {
		expired := p.cfg.MaxLifetime > 0 && now.Sub(ic.created) >= p.cfg.MaxLifetime
		stale := p.cfg.IdleTimeout > 0 && now.Sub(ic.idleSince) >= p.cfg.IdleTimeout
		if expired || stale {
			drop = append(drop, ic.conn)
			p.open--
			continue
		}
		keep = append(keep, ic)
	}
	p.idle = keep
	for range drop {
		p.wakeOneLocked()
	}
	p.mu.Unlock()
	for _, c := range drop {
		_ = c.Close()
	}
}
