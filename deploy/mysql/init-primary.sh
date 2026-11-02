#!/bin/bash
# Runs once, as root over the local socket, while the official image initializes the datadir.
set -euo pipefail

repl_pass="${MYSQL_REPLICATION_PASSWORD:-localdev}"
app_pass="${MYSQL_APP_PASSWORD:-localdev}"

sql=$(cat <<SQL
CREATE DATABASE IF NOT EXISTS app;
CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED BY '${repl_pass}';
GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';
CREATE USER IF NOT EXISTS 'app'@'%' IDENTIFIED BY '${app_pass}';
GRANT ALL PRIVILEGES ON app.* TO 'app'@'%';
FLUSH PRIVILEGES;

CREATE TABLE IF NOT EXISTS app.users (
  id BIGINT NOT NULL PRIMARY KEY,
  user_id BIGINT NOT NULL,
  name VARCHAR(255) NOT NULL,
  KEY idx_users_user_id (user_id)
);

CREATE TABLE IF NOT EXISTS app.orders (
  id BIGINT NOT NULL PRIMARY KEY,
  user_id BIGINT NOT NULL,
  amount INT NOT NULL,
  KEY idx_orders_user_id (user_id)
);
SQL
)

# The official image sometimes requires the root password during init and
# sometimes still accepts a passwordless socket connection. Try both.
# Keep the SQL in a variable so a failed first attempt does not consume it.
if [ -n "${MYSQL_ROOT_PASSWORD:-}" ]; then
  if printf '%s\n' "${sql}" | mysql --protocol=socket -uroot -p"${MYSQL_ROOT_PASSWORD}"; then
    exit 0
  fi
fi
printf '%s\n' "${sql}" | mysql --protocol=socket -uroot
