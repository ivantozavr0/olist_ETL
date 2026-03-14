CREATE TABLE IF NOT EXISTS fact_order_items
(
    order_id String,
    order_item_id UInt16,
    
    order_purchase_timestamp DateTime,
    
    order_status LowCardinality(String),

    product_id String,
    seller_id String,

    customer_unique_id String,
    customer_city String,
    customer_state LowCardinality(String),

    price Float64,
    freight_value Float64,
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(order_purchase_timestamp)
ORDER BY (order_id, order_item_id);
