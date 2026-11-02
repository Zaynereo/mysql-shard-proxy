#!/bin/bash
# Starts MySQL, then points this server at SOURCE_HOST with GTID auto-positioning.
set -euo pipefail

docker-entrypoint.sh "$@" &
pid=$!

pass="${MYSQL_ROOT_PASSWORD:-localdev}"
ready=0
for _ in $(seq 1 90); do
  if mysqladmin --protocol=tcp -h127.0.0.1 -uroot -p"${pass}" ping --silent >/dev/null 2>&1; then
    ready=1
    break
  fi
  if ! kill -0 "${pid}" 2>/dev/null; then
    echo "mysqld exited before it accepted connections" >&2
    wait "${pid}"
    exit 1
  fi
  sleep 2
done
if [ "${ready}" -ne 1 ]; then
  echo "timed out waiting for local mysqld" >&2
  exit 1
fi

repl_pass="${MYSQL_REPLICATION_PASSWORD:-localdev}"
app_pass="${MYSQL_APP_PASSWORD:-localdev}"
host="${SOURCE_HOST:?SOURCE_HOST is required}"

mysql --protocol=tcp -h127.0.0.1 -uroot -p"${pass}" <<SQL
CREATE USER IF NOT EXISTS 'app'@'%' IDENTIFIED BY '${app_pass}';
GRANT ALL PRIVILEGES ON app.* TO 'app'@'%';
FLUSH PRIVILEGES;
SQL

# A restarted container with a persisted datadir already has a replication channel.
configured=$(mysql --protocol=tcp -h127.0.0.1 -uroot -p"${pass}" -Nse "SHOW REPLICA STATUS" | wc -l)
if [ "${configured}" -eq 0 ]; then
  mysql --protocol=tcp -h127.0.0.1 -uroot -p"${pass}" <<SQL
CHANGE REPLICATION SOURCE TO
  SOURCE_HOST='${host}',
  SOURCE_PORT=3306,
  SOURCE_USER='repl',
  SOURCE_PASSWORD='${repl_pass}',
  SOURCE_AUTO_POSITION=1,
  GET_SOURCE_PUBLIC_KEY=1;
START REPLICA;
SQL
fi

wait "${pid}"
