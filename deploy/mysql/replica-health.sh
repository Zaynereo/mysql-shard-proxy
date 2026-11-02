#!/bin/bash
set -euo pipefail
pass="${MYSQL_ROOT_PASSWORD:-localdev}"
status=$(mysql --protocol=tcp -h127.0.0.1 -uroot -p"${pass}" -e "SHOW REPLICA STATUS\G")
printf '%s\n' "${status}" | grep -q "Replica_IO_Running: Yes"
printf '%s\n' "${status}" | grep -q "Replica_SQL_Running: Yes"
