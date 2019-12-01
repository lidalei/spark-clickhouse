CREATE TABLE IF NOT EXISTS default.items
(
    reviewerID Nullable(String), -- ID of the reviewer, e.g. A2SUAM1J3GNN3B
    asin String, -- ID of the product, e.g. 0000013714
    overall UInt8, -- rating of the product, e.g. 5
    unixReviewTime DateTime('UTC'), -- time of the review (unix time)
    unixReviewDate Date DEFAULT toDate(unixReviewTime) -- date of the review (unix time)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(unixReviewDate)
ORDER BY (overall, asin, unixReviewDate)