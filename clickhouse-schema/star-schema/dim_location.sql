CREATE TABLE IF NOT EXISTS dim_location
(
    city String,
    state LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY city;
