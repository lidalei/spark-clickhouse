-- create items table and its bufferred table
CREATE TABLE IF NOT EXISTS default.items_t
(
  reviewerID Nullable(String), -- ID of the reviewer, e.g. A2SUAM1J3GNN3B
  asin String, -- ID of the product, e.g. 0000013714
  overall UInt8, -- rating of the product, e.g. 5
  unixReviewTime DateTime('UTC'), -- time of the review (unix time)
  unixReviewDate Date DEFAULT toDate(unixReviewTime) -- date of the review (unix time)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(unixReviewDate)
ORDER BY (overall, asin, unixReviewDate);

-- Buffer(database, table, num_layers, min_time, max_time, min_rows, max_rows, min_bytes, max_bytes)
CREATE TABLE IF NOT EXISTS default.items AS default.items_t ENGINE = Buffer(default, items_t, 16, 10, 100, 10000, 1000000, 10000000, 100000000);

-- Check how many existing rows there are
-- SELECT COUNT(), 'rows in table items' FROM items;