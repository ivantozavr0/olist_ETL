CREATE TABLE IF NOT EXISTS dim_customer
(
    customer_unique_id String
)
ENGINE = MergeTree
ORDER BY customer_unique_id;
