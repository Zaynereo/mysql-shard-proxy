package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
)

type stubConn struct {
	id     int
	closed atomic.Bool
}

func (s *stubConn) Execute(string, ...any) (*mysql.Result, error) { return &mysql.Result{}, nil }
func (s *stubConn) UseDB(string) error                            { return nil }
func (s *stubConn) Prepare(string) (Stmt, error)                  { return nil, errors.New("not used") }
func (s *stubConn) FieldList(string, string) ([]*mysql.Field, error) {
	return nil, nil
}
func (s *stubConn) Close() error {
	s.closed.Store(true)
	return nil
}

func TestPoolReusesAndLimits(t *testing.T) {
	var n atomic.Int32
	p := New(Config{MaxOpen: 2, MaxIdle: 2, MaxLifetime: time.Minute, IdleTimeout: time.Minute}, func(context.Context) (Conn, error) {
		id := int(n.Add(1))
		return &stubConn{id: id}, nil
	})
	defer p.Close()

	ctx := context.Background()
	a, err := p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id := a.Conn().(*stubConn).id
	type result struct {
		lease *Lease
		err   error
	}
	got := make(chan result, 1)
	go func() {
		lease, err := p.Get(ctx)
		got <- result{lease: lease, err: err}
	}()
	time.Sleep(40 * time.Millisecond)
	a.Release(false)
	res := <-got
	if res.err != nil {
		t.Fatal(res.err)
	}
	if res.lease.Conn().(*stubConn).id != id {
		t.Fatalf("waiter got conn %d, want reused %d", res.lease.Conn().(*stubConn).id, id)
	}
	res.lease.Release(false)
	b.Release(false)
	stats := p.Stats()
	if stats.Open != 2 || stats.InUse != 0 {
		t.Fatalf("stats %+v", stats)
	}
}

func TestPoolAcquireTimeout(t *testing.T) {
	p := New(Config{MaxOpen: 1, MaxIdle: 1, MaxLifetime: time.Minute, IdleTimeout: time.Minute}, func(context.Context) (Conn, error) {
		return &stubConn{}, nil
	})
	defer p.Close()
	held, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release(false)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := p.Get(ctx); err == nil {
		t.Fatal("expected timeout")
	}
	if time.Since(start) < 30*time.Millisecond {
		t.Fatal("returned before timeout")
	}
}

func TestPoolIdleTimeoutAndClose(t *testing.T) {
	p := New(Config{MaxOpen: 1, MaxIdle: 1, MaxLifetime: time.Minute, IdleTimeout: 30 * time.Millisecond}, func(context.Context) (Conn, error) {
		return &stubConn{}, nil
	})
	lease, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	conn := lease.Conn().(*stubConn)
	lease.Release(false)
	time.Sleep(80 * time.Millisecond)
	if !conn.closed.Load() {
		t.Fatal("idle connection was not reaped")
	}
	p.Close()
	if _, err := p.Get(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v", err)
	}
}

func TestPoolConcurrent(t *testing.T) {
	p := New(Config{MaxOpen: 4, MaxIdle: 4, MaxLifetime: time.Minute, IdleTimeout: time.Minute}, func(context.Context) (Conn, error) {
		return &stubConn{}, nil
	})
	defer p.Close()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 30; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				lease, err := p.Get(ctx)
				cancel()
				if err != nil {
					t.Errorf("get: %v", err)
					return
				}
				lease.Release(j%7 == 0)
			}
		}()
	}
	wg.Wait()
}

func TestPoolFactoryError(t *testing.T) {
	p := New(Config{MaxOpen: 1, MaxIdle: 1, MaxLifetime: time.Minute, IdleTimeout: time.Minute}, func(context.Context) (Conn, error) {
		return nil, errors.New("dial failed")
	})
	defer p.Close()
	_, err := p.Get(context.Background())
	if err == nil || err.Error() != "dial failed" {
		t.Fatalf("got %v", err)
	}
	if stats := p.Stats(); stats.Open != 0 {
		t.Fatalf("open leaked: %+v", stats)
	}
}
