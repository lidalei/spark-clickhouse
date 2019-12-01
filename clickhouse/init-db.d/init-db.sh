#!/bin/bash
# This script file is executed after *.sql files being executed
set -e

echo "The number of rows in table items and metadata is respectively"
clickhouse client --user $CLICKHOUSE_USER --password $CLICKHOUSE_PASSWORD -n -m <<-EOSQL
  SELECT COUNT() FROM items;
  SELECT COUNT() FROM metadata;
EOSQL