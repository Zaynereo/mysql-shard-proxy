package proxy

import (
	"errors"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/Zaynereo/mysql-shard-proxy/internal/pool"
	"github.com/Zaynereo/mysql-shard-proxy/internal/sqlx"
)

func mapParseErr(err error) error {
	var unsupported *sqlx.UnsupportedError
	if errors.As(err, &unsupported) {
		return reject(unsupported.Error())
	}
	return mysql.NewError(mysql.ER_PARSE_ERROR, err.Error())
}

func reject(msg string) error {
	return mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, msg)
}

func wrapErr(err error) error {
	if err == nil {
		return nil
	}
	if my := mysqlError(err); my != nil {
		return my
	}
	if errors.Is(err, pool.ErrClosed) {
		return mysql.NewError(mysql.ER_UNKNOWN_ERROR, "backend pool is closed")
	}
	return mysql.NewError(mysql.ER_UNKNOWN_ERROR, err.Error())
}

func isMySQL(err error) bool {
	return mysqlError(err) != nil
}

func mysqlError(err error) *mysql.MyError {
	var my *mysql.MyError
	if errors.As(err, &my) {
		return my
	}
	type causer interface{ Cause() error }
	seen := 0
	for err != nil && seen < 8 {
		seen++
		c, ok := err.(causer)
		if !ok {
			break
		}
		err = c.Cause()
		if errors.As(err, &my) {
			return my
		}
	}
	return nil
}

func truncateSQL(sql string) string {
	sql = strings.Join(strings.Fields(sql), " ")
	if len(sql) > 240 {
		return sql[:240] + "..."
	}
	return sql
}

func allIdx(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}
