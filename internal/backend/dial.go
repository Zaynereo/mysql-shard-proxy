// Package backend dials MySQL servers for the proxy pools and health checks.
package backend

import (
	"context"
	"fmt"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/Zaynereo/mysql-shard-proxy/internal/config"
	"github.com/Zaynereo/mysql-shard-proxy/internal/pool"
)

type mysqlConn struct {
	*client.Conn
}

type mysqlStmt struct {
	*client.Stmt
}

// Open dials one backend connection.
func Open(ctx context.Context, b config.Backend, readTimeout, writeTimeout, dialTimeout time.Duration) (pool.Conn, error) {
	conn, err := client.ConnectWithContext(ctx, b.Addr, b.User, b.Password, b.Database, dialTimeout, func(c *client.Conn) error {
		c.ReadTimeout = readTimeout
		c.WriteTimeout = writeTimeout
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", b.Addr, err)
	}
	return mysqlConn{Conn: conn}, nil
}

func (c mysqlConn) Prepare(query string) (pool.Stmt, error) {
	st, err := c.Conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	return mysqlStmt{Stmt: st}, nil
}

func (s mysqlStmt) ParamCount() int  { return s.ParamNum() }
func (s mysqlStmt) ColumnCount() int { return s.ColumnNum() }
func (s mysqlStmt) Execute(args ...any) (*mysql.Result, error) {
	return s.Stmt.Execute(args...)
}
