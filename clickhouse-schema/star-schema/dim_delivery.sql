CREATE TABLE IF NOT EXISTS dim_delivery
(
    order_id String,
    order_purchase_timestamp DateTime,
    delivery_days Nullable(UInt16),
    order_delivered_customer_date Nullable(DateTime),
    order_approved_at Nullable(DateTime),
    order_delivered_carrier_date Nullable(DateTime),
    order_estimated_delivery_date Nullable(DateTime),
    is_delivery_date_missing Bool
)
ENGINE = MergeTree
ORDER BY order_id;
