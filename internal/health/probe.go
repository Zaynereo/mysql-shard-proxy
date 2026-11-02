package health

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/Zaynereo/mysql-shard-proxy/internal/config"
)

// ProbeMySQL dials backend and checks it.
// Primaries must answer SELECT 1. Replicas must be replicating within the caller's lag policy.
func ProbeMySQL(ctx context.Context, backend config.Backend, replica bool, timeout time.Duration) (Sample, error) {
	if timeout <= 0 {
		timeout = time.Second
	}
	dialCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := client.ConnectWithContext(dialCtx, backend.Addr, backend.User, backend.Password, backend.Database, timeout)
	if err != nil {
		return Sample{}, err
	}
	defer func() { _ = conn.Close() }()
	if !replica {
		if _, err := conn.Execute("SELECT 1"); err != nil {
			return Sample{}, err
		}
		return Sample{}, nil
	}
	res, err := conn.Execute("SHOW REPLICA STATUS")
	if err != nil {
		res, err = conn.Execute("SHOW SLAVE STATUS")
		if err != nil {
			return Sample{}, err
		}
	}
	return parseReplicaStatus(res)
}

func parseReplicaStatus(res *mysql.Result) (Sample, error) {
	if res == nil || res.Resultset == nil || res.RowNumber() == 0 {
		return Sample{ReplicationRunning: false}, nil
	}
	ioRunning, err := fieldString(res, "Replica_IO_Running", "Slave_IO_Running")
	if err != nil {
		return Sample{}, err
	}
	sqlRunning, err := fieldString(res, "Replica_SQL_Running", "Slave_SQL_Running")
	if err != nil {
		return Sample{}, err
	}
	sample := Sample{
		ReplicationRunning: strings.EqualFold(ioRunning, "Yes") && strings.EqualFold(sqlRunning, "Yes"),
	}
	idx, ok := fieldIndex(res, "Seconds_Behind_Source", "Seconds_Behind_Master")
	if !ok {
		return sample, nil
	}
	null, err := res.IsNull(0, idx)
	if err != nil {
		return Sample{}, err
	}
	if null {
		return sample, nil
	}
	seconds, err := res.GetInt(0, idx)
	if err != nil {
		return Sample{}, err
	}
	if seconds < 0 {
		return Sample{}, fmt.Errorf("negative replication lag %d", seconds)
	}
	sample.Lag = time.Duration(seconds) * time.Second
	sample.LagKnown = true
	return sample, nil
}

func fieldString(res *mysql.Result, names ...string) (string, error) {
	idx, ok := fieldIndex(res, names...)
	if !ok {
		return "", fmt.Errorf("replication status is missing %s", strings.Join(names, "/"))
	}
	return res.GetString(0, idx)
}

func fieldIndex(res *mysql.Result, names ...string) (int, bool) {
	for i, f := range res.Fields {
		for _, name := range names {
			if strings.EqualFold(string(f.Name), name) {
				return i, true
			}
		}
	}
	return 0, false
}
