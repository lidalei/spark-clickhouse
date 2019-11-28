#!/bin/bash
set -e

clickhouse client -n <<-EOSQL
  -- create items table and its bufferred table
  CREATE TABLE IF NOT EXISTS default.items_t
  (
      reviewerID String, -- ID of the reviewer, e.g. A2SUAM1J3GNN3B
      asin String, -- ID of the product, e.g. 0000013714
      overall UInt8, -- rating of the product, e.g. 5
      unixReviewTime DateTime('UTC'), -- time of the review (unix time)
      unixReviewDate Date DEFAULT toDate(unixReviewTime) -- date of the review (unix time)
  ) ENGINE = MergeTree()
  PARTITION BY toYYYYMM(unixReviewDate)
  ORDER BY (overall, asin, unixReviewDate);

  -- Buffer(database, table, num_layers, min_time, max_time, min_rows, max_rows, min_bytes, max_bytes)
  CREATE TABLE IF NOT EXISTS default.items AS default.items_t ENGINE = Buffer(default, items_t, 16, 10, 100, 10000, 1000000, 10000000, 100000000)

  -- create metadata table and its bufferred table
  CREATE TABLE IF NOT EXISTS default.metadata_t
  (
      asin String, -- ID of the product, e.g. 0000013714
      -- price Nullable(Decimal(10, 2)), -- price in decimal
      price_in_cents Nullable(UInt32), -- price in unit of cents, e.g. 5.30 is stored as 530
      same_viewed_bought UInt8 -- if also_bought identical to also_viewed, 0 means no, any other value means yes
  ) ENGINE = MergeTree()
  PARTITION BY substring(asin, 1, 1)  -- use first character of asin as partition key, not for speed up, only for data manipulation
  ORDER BY (same_viewed_bought, asin);

  -- Buffer(database, table, num_layers, min_time, max_time, min_rows, max_rows, min_bytes, max_bytes)
  CREATE TABLE IF NOT EXISTS default.metadata AS default.metadata_t ENGINE = Buffer(default, metadata_t, 16, 10, 100, 10000, 1000000, 10000000, 100000000);
EOSQL