CREATE TABLE IF NOT EXISTS default.items
(
    asin String, -- ID of the product, e.g. 0000013714
    price_in_cents Nullable(UInt32), -- price in unit of cents, e.g. 5.30 is stored as 530
    -- price Decimal(10, 2) -- price in decimal
    same_viewed_bought Boolean -- if also_bought identical to also_viewed
) ENGINE = MergeTree()
PARTITION BY toFixedString(asin, 3)  -- use prefix of asin as partition key, it might need tuning with experiment
ORDER BY (asin)