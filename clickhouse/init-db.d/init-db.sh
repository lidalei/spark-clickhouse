#!/bin/bash
# This script file is executed after *.sql files being executed
set -e

echo "Simple tests"
clickhouse client --user $CLICKHOUSE_USER --password $CLICKHOUSE_PASSWORD -n -m <<-EOSQL
  SELECT 1 + 1;
  SELECT 1 - 1;
EOSQL