CREATE TABLE IF NOT EXISTS dim_reviews
(
    order_id String,
    review_score Nullable(Float32)
)
ENGINE = MergeTree
ORDER BY order_id;
