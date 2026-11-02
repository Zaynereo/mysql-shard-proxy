package proxy

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"github.com/Zaynereo/mysql-shard-proxy/internal/pool"
	"github.com/Zaynereo/mysql-shard-proxy/internal/route"
	"github.com/Zaynereo/mysql-shard-proxy/internal/sqlx"
)

// tracked remembers which session prelude has been applied to a pooled connection.
type tracked struct {
	pool.Conn
	fp string
}

type held struct {
	shard int
	lease *pool.Lease
}

type prepared struct {
	query    string
	analysis *sqlx.Analysis
}

type session struct {
	app        *App
	db         string
	sets       []string
	autocommit bool
	inTxn      bool
	explicit   bool
	backendTxn bool
	sawWrite   bool
	txnShard   int
	rywUntil   time.Time
	pin        *held
}

func newSession(app *App) *session {
	return &session{app: app, autocommit: true, txnShard: -1}
}

var _ server.Handler = (*session)(nil)

func (s *session) UseDB(db string) error {
	_, err := s.useDB(db)
	return err
}

func (s *session) HandleQuery(query string) (*mysql.Result, error) {
	analysis, err := sqlx.Parse(query, s.app.cfg.ShardKey)
	if err != nil {
		s.app.log.Debug("parse failed", "err", err, "sql", truncateSQL(query))
		return nil, mapParseErr(err)
	}
	s.app.log.Debug("query", "op", analysis.Kind.String(), "sql", truncateSQL(query))
	switch analysis.Kind {
	case sqlx.KindEmpty:
		return s.ok(), nil
	case sqlx.KindUse:
		return s.useDB(analysis.UseDB)
	case sqlx.KindSet:
		return s.handleSet(query, analysis)
	case sqlx.KindBegin:
		return s.handleBegin()
	case sqlx.KindCommit:
		return s.handleCommit()
	case sqlx.KindRollback:
		return s.handleRollback()
	default:
		return s.dispatch(analysis, strings.TrimSpace(query), nil)
	}
}

func (s *session) HandleFieldList(table, wildcard string) ([]*mysql.Field, error) {
	lease, err := s.borrow(s.app.primary[0].pool)
	if err != nil {
		return nil, wrapErr(err)
	}
	defer lease.Release(false)
	fields, err := lease.Conn().FieldList(table, wildcard)
	if err != nil {
		return nil, wrapErr(err)
	}
	return fields, nil
}

func (s *session) HandleStmtPrepare(query string) (int, int, interface{}, error) {
	analysis, err := sqlx.Parse(query, s.app.cfg.ShardKey)
	if err != nil {
		return 0, 0, nil, mapParseErr(err)
	}
	lease, err := s.borrow(s.app.primary[0].pool)
	if err != nil {
		return 0, 0, nil, wrapErr(err)
	}
	stmt, err := lease.Conn().Prepare(query)
	if err != nil {
		lease.Release(!isMySQL(err))
		return 0, 0, nil, wrapErr(err)
	}
	params, cols := stmt.ParamCount(), stmt.ColumnCount()
	closeErr := stmt.Close()
	lease.Release(closeErr != nil && !isMySQL(closeErr))
	if closeErr != nil {
		return 0, 0, nil, wrapErr(closeErr)
	}
	return params, cols, &prepared{query: query, analysis: analysis}, nil
}

func (s *session) HandleStmtExecute(ctx interface{}, _ string, args []interface{}) (*mysql.Result, error) {
	stmt, ok := ctx.(*prepared)
	if !ok || stmt == nil || stmt.analysis == nil {
		return nil, reject("unknown prepared statement")
	}
	bound := make([]any, len(args))
	copy(bound, args)
	return s.dispatch(stmt.analysis, stmt.query, bound)
}

func (s *session) HandleStmtClose(interface{}) error { return nil }

func (s *session) HandleOtherCommand(cmd byte, _ []byte) error {
	return reject(fmt.Sprintf("command 0x%x is not supported", cmd))
}

func (s *session) Close() {
	if s.pin == nil {
		return
	}
	if s.backendTxn || !s.autocommit {
		_, _ = s.pin.lease.Conn().Execute("ROLLBACK")
	}
	s.releasePin(true)
}

func (s *session) useDB(db string) (*mysql.Result, error) {
	s.db = db
	if s.pin != nil {
		if err := s.pin.lease.Conn().UseDB(db); err != nil {
			return nil, wrapErr(err)
		}
		s.syncPinFingerprint()
	}
	return s.ok(), nil
}

func (s *session) handleSet(raw string, analysis *sqlx.Analysis) (*mysql.Result, error) {
	if analysis.SetGlobal {
		return nil, reject("SET GLOBAL is not supported")
	}
	if analysis.Autocommit != nil {
		if err := s.setAutocommit(*analysis.Autocommit); err != nil {
			return nil, err
		}
	}
	stmt := strings.TrimSpace(raw)
	stmt = strings.TrimRight(stmt, "; \t\r\n")
	s.sets = append(s.sets, stmt)
	if s.pin != nil {
		if _, err := s.pin.lease.Conn().Execute(stmt); err != nil {
			return nil, wrapErr(err)
		}
		s.syncPinFingerprint()
	}
	s.app.metrics.Observe("none", "none", "set", nil, 0)
	return s.ok(), nil
}

func (s *session) setAutocommit(on bool) error {
	if on == s.autocommit {
		return nil
	}
	if on && s.pin != nil && (s.backendTxn || !s.autocommit) {
		if _, err := s.pin.lease.Conn().Execute("COMMIT"); err != nil {
			return wrapErr(err)
		}
		if s.sawWrite {
			s.noteWrite()
		}
		s.backendTxn = false
		s.sawWrite = false
		s.explicit = false
		s.inTxn = false
		s.txnShard = -1
		if !s.rywActive() {
			s.releasePin(true)
		}
	}
	if !on {
		s.inTxn = true
		s.explicit = false
	}
	s.autocommit = on
	return nil
}

func (s *session) handleBegin() (*mysql.Result, error) {
	if s.pin != nil && s.backendTxn {
		if _, err := s.pin.lease.Conn().Execute("COMMIT"); err != nil {
			return nil, wrapErr(err)
		}
		if s.sawWrite {
			s.noteWrite()
		}
		s.sawWrite = false
		s.backendTxn = false
		s.txnShard = -1
		s.releasePin(true)
	}
	s.inTxn = true
	s.explicit = true
	return s.ok(), nil
}

func (s *session) handleCommit() (*mysql.Result, error) {
	if s.pin != nil && (s.backendTxn || !s.autocommit) {
		res, err := s.pin.lease.Conn().Execute("COMMIT")
		if err != nil {
			return nil, wrapErr(err)
		}
		if s.sawWrite {
			s.noteWrite()
		}
		s.sawWrite = false
		s.explicit = false
		s.backendTxn = false
		if s.autocommit {
			s.inTxn = false
			s.txnShard = -1
			if !s.rywActive() {
				s.releasePin(true)
			}
		} else {
			s.inTxn = true
			s.txnShard = s.pin.shard
		}
		if res == nil {
			return s.ok(), nil
		}
		res.Status |= s.status()
		return res, nil
	}
	if s.autocommit {
		s.inTxn = false
		s.explicit = false
		s.txnShard = -1
	}
	return s.ok(), nil
}

func (s *session) handleRollback() (*mysql.Result, error) {
	if s.pin != nil && (s.backendTxn || !s.autocommit) {
		res, err := s.pin.lease.Conn().Execute("ROLLBACK")
		if err != nil {
			return nil, wrapErr(err)
		}
		s.sawWrite = false
		s.explicit = false
		s.backendTxn = false
		if s.autocommit {
			s.inTxn = false
			s.txnShard = -1
			s.releasePin(true)
		} else {
			s.inTxn = true
			s.txnShard = s.pin.shard
		}
		if res == nil {
			return s.ok(), nil
		}
		res.Status |= s.status()
		return res, nil
	}
	if s.autocommit {
		s.inTxn = false
		s.explicit = false
		s.txnShard = -1
	}
	return s.ok(), nil
}

func (s *session) dispatch(analysis *sqlx.Analysis, query string, args []any) (*mysql.Result, error) {
	if analysis.Kind == sqlx.KindDDL && !analysis.Explain {
		plan := route.Plan{Shards: allIdx(len(s.app.cfg.Shards)), Role: route.Primary, Fanout: true}
		return s.execPlan(plan, analysis, query, args)
	}
	if len(analysis.Tables) > 1 {
		return nil, reject("multiple tables are not supported")
	}
	sharded := false
	if len(analysis.Tables) == 1 {
		sharded = s.app.cfg.ShardedTable(analysis.Tables[0])
	}
	var match sqlx.Match
	if sharded {
		var err error
		match, err = analysis.Match(s.app.cfg.ShardKey, args)
		if err != nil {
			return nil, reject(err.Error())
		}
	}
	meta := analysis.Kind == sqlx.KindShow || analysis.Kind == sqlx.KindDescribe || analysis.Kind == sqlx.KindDo
	txnShard := -1
	if s.inTxn {
		txnShard = s.txnShard
	}
	plan, err := route.Build(route.Input{
		ShardCount:          len(s.app.cfg.Shards),
		Keys:                match.Keys,
		PerRow:              match.PerRow,
		All:                 sharded && match.All,
		Sharded:             sharded,
		Write:               analysis.Kind.WritesData() && !analysis.Explain,
		PreferPrimary:       analysis.Kind.WritesData() || analysis.Locking || meta || s.rywActive() || s.inTxn,
		InTxn:               s.inTxn,
		TxnShard:            txnShard,
		AllowMultiShardRead: s.app.cfg.AllowMultiRead,
		AllowScatterRead:    s.app.cfg.AllowScatterRead,
		UnsafeMerge:         analysis.Flags.UnsafeMerge(),
	})
	if err != nil {
		return nil, reject(err.Error())
	}
	if len(args) > 0 && len(plan.Shards) > 1 {
		return nil, reject("multi-shard prepared statements are not supported")
	}
	return s.execPlan(plan, analysis, query, args)
}

func (s *session) execPlan(plan route.Plan, analysis *sqlx.Analysis, query string, args []any) (*mysql.Result, error) {
	hold := len(plan.Shards) == 1
	if len(plan.Shards) == 1 {
		return s.execOne(plan.Shards[0], plan.Role, analysis, query, args, hold)
	}
	parts := make([]*mysql.Result, 0, len(plan.Shards))
	for _, idx := range plan.Shards {
		res, err := s.execOne(idx, plan.Role, analysis, query, args, false)
		if err != nil {
			return nil, err
		}
		parts = append(parts, res)
	}
	if plan.MergeRows {
		merged, err := mergeRows(parts)
		if err != nil {
			return nil, err
		}
		merged.Status |= s.status()
		return merged, nil
	}
	return mergeOK(parts, s.status()), nil
}

func (s *session) execOne(shard int, role string, analysis *sqlx.Analysis, query string, args []any, hold bool) (*mysql.Result, error) {
	start := time.Now()
	s.app.metrics.Begin()
	defer s.app.metrics.End()
	modifies := analysis.Kind.WritesData() && !analysis.Explain
	lease, pinned, actual, err := s.connFor(shard, role)
	shardName := s.app.primary[shard].shard
	if err != nil {
		s.app.metrics.Observe(shardName, role, analysis.Kind.String(), err, time.Since(start))
		s.app.log.Info("query failed", "shard", shardName, "role", role, "op", analysis.Kind.String(), "err", err)
		return nil, wrapErr(err)
	}
	res, err := runQuery(lease.Conn(), query, args)
	broken := err != nil && !isMySQL(err)
	wrapped := wrapErr(err)
	s.app.metrics.Observe(shardName, actual, analysis.Kind.String(), wrapped, time.Since(start))
	if wrapped != nil {
		s.app.log.Info("query failed", "shard", shardName, "role", actual, "op", analysis.Kind.String(), "err", wrapped, "duration", time.Since(start))
		if pinned {
			if broken {
				s.backendTxn = false
				s.releasePin(true)
			}
		} else {
			lease.Release(broken)
		}
		return nil, wrapped
	}
	if modifies {
		s.noteWrite()
		if s.inTxn {
			s.sawWrite = true
		}
	}
	if pinned {
		return res, nil
	}
	if modifies && hold && !s.inTxn && s.app.cfg.ReadYourWrites.Duration > 0 {
		if s.pin != nil {
			s.releasePin(true)
		}
		s.pin = &held{shard: shard, lease: lease}
		return res, nil
	}
	// Writes with a zero read-your-writes window close the connection so
	// LAST_INSERT_ID and session residue are not visible to the next client.
	lease.Release(modifies)
	return res, nil
}

func (s *session) connFor(shard int, role string) (*pool.Lease, bool, string, error) {
	s.expirePin()
	if s.pin != nil && s.pin.shard == shard && (s.inTxn || s.rywActive()) {
		return s.pin.lease, true, route.Primary, nil
	}
	if s.inTxn {
		if s.pin != nil && s.pin.shard != shard {
			return nil, false, route.Primary, reject("cross-shard transactions are not supported")
		}
		if s.txnShard >= 0 && s.txnShard != shard {
			return nil, false, route.Primary, reject("cross-shard transactions are not supported")
		}
		lease, err := s.borrow(s.app.primary[shard].pool)
		if err != nil {
			return nil, false, route.Primary, err
		}
		if s.explicit {
			if _, err := lease.Conn().Execute("BEGIN"); err != nil {
				lease.Release(true)
				return nil, false, route.Primary, err
			}
		}
		s.pin = &held{shard: shard, lease: lease}
		s.backendTxn = true
		s.txnShard = shard
		return lease, true, route.Primary, nil
	}
	actual := role
	var bp *backendPool
	if role == route.Replica {
		bp = s.pickReplica(shard)
		if bp == nil {
			if !s.app.cfg.FallbackToPrimary() {
				return nil, false, route.Replica, reject("no healthy replica")
			}
			s.app.metrics.Fallback(s.app.primary[shard].shard)
			s.app.log.Debug("read fallback", "shard", s.app.primary[shard].shard)
			bp = s.app.primary[shard]
			actual = route.Primary
		}
	} else {
		bp = s.app.primary[shard]
		actual = route.Primary
	}
	lease, err := s.borrow(bp.pool)
	if err != nil {
		return nil, false, actual, err
	}
	return lease, false, actual, nil
}

func (s *session) pickReplica(shard int) *backendPool {
	var healthy []*backendPool
	for _, rep := range s.app.replicas[shard] {
		if s.app.health.Healthy(rep.shard, rep.name) {
			healthy = append(healthy, rep)
		}
	}
	if len(healthy) == 0 {
		return nil
	}
	n := s.app.rr[shard].Add(1)
	return healthy[int(n%uint64(len(healthy)))]
}

func (s *session) borrow(p *pool.Pool) (*pool.Lease, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.app.cfg.Pool.AcquireTimeout.Duration)
	defer cancel()
	want := s.fingerprint()
	var lastErr error
	for attempt := 0; attempt < 8; attempt++ {
		lease, err := p.Get(ctx)
		if err != nil {
			return nil, err
		}
		tc, ok := lease.Conn().(*tracked)
		if !ok {
			lease.Release(true)
			return nil, reject("internal connection type mismatch")
		}
		if tc.fp == want {
			return lease, nil
		}
		if tc.fp != "" {
			lease.Release(true)
			continue
		}
		if err := s.replay(tc); err != nil {
			lease.Release(true)
			lastErr = err
			continue
		}
		tc.fp = want
		return lease, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, reject("no clean backend connection")
}

func (s *session) replay(tc *tracked) error {
	for _, query := range s.sets {
		if _, err := tc.Execute(query); err != nil {
			return err
		}
	}
	if s.db != "" {
		if err := tc.UseDB(s.db); err != nil {
			return err
		}
	}
	return nil
}

func (s *session) fingerprint() string {
	return s.db + "\n" + strings.Join(s.sets, "\n")
}

func (s *session) syncPinFingerprint() {
	if s.pin == nil {
		return
	}
	if tc, ok := s.pin.lease.Conn().(*tracked); ok {
		tc.fp = s.fingerprint()
	}
}

func (s *session) expirePin() {
	if s.pin == nil || s.inTxn || s.backendTxn {
		return
	}
	if s.rywActive() {
		return
	}
	s.releasePin(true)
}

func (s *session) releasePin(broken bool) {
	if s.pin == nil {
		return
	}
	s.pin.lease.Release(broken)
	s.pin = nil
}

func (s *session) noteWrite() {
	if s.app.cfg.ReadYourWrites.Duration > 0 {
		s.rywUntil = time.Now().Add(s.app.cfg.ReadYourWrites.Duration)
	}
}

func (s *session) rywActive() bool {
	return !s.rywUntil.IsZero() && time.Now().Before(s.rywUntil)
}

func (s *session) status() uint16 {
	var status uint16
	if s.autocommit {
		status |= mysql.SERVER_STATUS_AUTOCOMMIT
	}
	if s.inTxn {
		status |= mysql.SERVER_STATUS_IN_TRANS
	}
	return status
}

func (s *session) ok() *mysql.Result {
	return &mysql.Result{Status: s.status()}
}

func runQuery(conn pool.Conn, query string, args []any) (*mysql.Result, error) {
	if len(args) == 0 {
		return conn.Execute(query)
	}
	stmt, err := conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = stmt.Close() }()
	return stmt.Execute(args...)
}
