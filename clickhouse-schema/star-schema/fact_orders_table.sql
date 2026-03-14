CREATE TABLE IF NOT EXISTS fact_orders
(
    order_id String,
    
    order_purchase_timestamp DateTime,
    
    order_status LowCardinality(String),

    customer_unique_id String,
    customer_city String,
    customer_state LowCardinality(String),

    payment_value Nullable(Float64),
    review_score Nullable(Float32)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(order_purchase_timestamp)
ORDER BY (order_id);
