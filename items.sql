CREATE TABLE IF NOT EXISTS default.items
(
    reviewerID String, -- ID of the reviewer, e.g. A2SUAM1J3GNN3B
    asin String, -- ID of the product, e.g. 0000013714
    overall UInt8, -- rating of the product, e.g. 5
    unixReviewTime DateTime('UTC') -- time of the review (unix time)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(unixReviewTime)
ORDER BY (asin, unixReviewTime)