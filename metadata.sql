CREATE TABLE IF NOT EXISTS default.metadata
(
    asin String, -- ID of the product, e.g. 0000013714
    -- price Nullable(Decimal(10, 2)), -- price in decimal
    price_in_cents Nullable(UInt32), -- price in unit of cents, e.g. 5.30 is stored as 530
    same_viewed_bought UInt8 -- if also_bought identical to also_viewed, 0 means no, any other value means yes
) ENGINE = MergeTree()
PARTITION BY substring(asin, 1, 1)  -- use first character of asin as partition key, not for speed up, only for data manupilation
ORDER BY (asin)