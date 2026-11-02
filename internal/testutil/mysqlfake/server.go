// Package mysqlfake is an in-process MySQL server for proxy tests.
package mysqlfake

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
)

// Server accepts MySQL clients and records statements.
type Server struct {
	Addr string

	ln       net.Listener
	mysql    *server.Server
	user     string
	password string

	mu      sync.Mutex
	queries []string

	lagSeconds  atomic.Int64
	replRunning atomic.Bool
	closed      atomic.Bool
}

// Start listens on 127.0.0.1:0.
func Start(user, password string) (*Server, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	s := &Server{
		Addr:     ln.Addr().String(),
		ln:       ln,
		user:     user,
		password: password,
		mysql:    server.NewServer("8.0.36", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, nil, nil),
	}
	s.replRunning.Store(true)
	go s.accept()
	return s, nil
}

// SetReplica controls SHOW REPLICA STATUS.
func (s *Server) SetReplica(running bool, lag time.Duration) {
	s.replRunning.Store(running)
	s.lagSeconds.Store(int64(lag / time.Second))
}

// Queries returns statements in arrival order, including health probes.
func (s *Server) Queries() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.queries))
	copy(out, s.queries)
	return out
}

// Close stops the listener.
func (s *Server) Close() {
	s.closed.Store(true)
	_ = s.ln.Close()
}

func (s *Server) accept() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		go s.serve(conn)
	}
}

func (s *Server) serve(conn net.Conn) {
	h := &handler{s: s}
	sc, err := s.mysql.NewConn(conn, s.user, s.password, h)
	if err != nil {
		_ = conn.Close()
		return
	}
	for {
		if err := sc.HandleCommand(); err != nil {
			return
		}
	}
}

func (s *Server) record(q string) {
	s.mu.Lock()
	s.queries = append(s.queries, q)
	s.mu.Unlock()
}

type handler struct {
	s *Server
}

func (h *handler) UseDB(db string) error {
	h.s.record("USE " + db)
	return nil
}

func (h *handler) HandleQuery(query string) (*mysql.Result, error) {
	h.s.record(query)
	upper := strings.ToUpper(strings.TrimSpace(query))
	switch {
	case strings.HasPrefix(upper, "SHOW REPLICA STATUS"):
		return h.s.replicaResult("Replica_IO_Running", "Replica_SQL_Running", "Seconds_Behind_Source")
	case strings.HasPrefix(upper, "SHOW SLAVE STATUS"):
		return h.s.replicaResult("Slave_IO_Running", "Slave_SQL_Running", "Seconds_Behind_Master")
	case strings.HasPrefix(upper, "SELECT"), strings.HasPrefix(upper, "SHOW"), strings.HasPrefix(upper, "EXPLAIN"):
		rs, err := mysql.BuildSimpleTextResultset([]string{"v"}, [][]interface{}{{int64(1)}})
		if err != nil {
			return nil, err
		}
		return mysql.NewResult(rs), nil
	default:
		return &mysql.Result{AffectedRows: 1, InsertId: 1, Status: mysql.SERVER_STATUS_AUTOCOMMIT}, nil
	}
}

func (h *handler) HandleFieldList(string, string) ([]*mysql.Field, error) {
	return []*mysql.Field{{Name: []byte("id")}}, nil
}

func (h *handler) HandleStmtPrepare(query string) (int, int, interface{}, error) {
	h.s.record("PREPARE " + query)
	params := strings.Count(query, "?")
	cols := 0
	upper := strings.ToUpper(strings.TrimSpace(query))
	if strings.HasPrefix(upper, "SELECT") || strings.HasPrefix(upper, "SHOW") || strings.HasPrefix(upper, "EXPLAIN") {
		cols = 1
	}
	return params, cols, query, nil
}

func (h *handler) HandleStmtExecute(_ interface{}, query string, args []interface{}) (*mysql.Result, error) {
	h.s.record(fmt.Sprintf("EXECUTE %s %v", query, args))
	upper := strings.ToUpper(strings.TrimSpace(query))
	if strings.HasPrefix(upper, "SELECT") || strings.HasPrefix(upper, "SHOW") || strings.HasPrefix(upper, "EXPLAIN") {
		rs, err := mysql.BuildSimpleBinaryResultset([]string{"v"}, [][]interface{}{{int64(1)}})
		if err != nil {
			return nil, err
		}
		return mysql.NewResult(rs), nil
	}
	return &mysql.Result{AffectedRows: 1, InsertId: 1, Status: mysql.SERVER_STATUS_AUTOCOMMIT}, nil
}

func (h *handler) HandleStmtClose(interface{}) error { return nil }

func (h *handler) HandleOtherCommand(byte, []byte) error {
	return mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, "command is not supported")
}

func (s *Server) replicaResult(ioCol, sqlCol, lagCol string) (*mysql.Result, error) {
	ioState := "Yes"
	sqlState := "Yes"
	var lag interface{} = s.lagSeconds.Load()
	if !s.replRunning.Load() {
		ioState = "No"
		sqlState = "No"
		lag = nil
	}
	rs, err := mysql.BuildSimpleTextResultset(
		[]string{ioCol, sqlCol, lagCol},
		[][]interface{}{{ioState, sqlState, lag}},
	)
	if err != nil {
		return nil, err
	}
	return mysql.NewResult(rs), nil
}
