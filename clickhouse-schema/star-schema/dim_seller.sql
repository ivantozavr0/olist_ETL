CREATE TABLE IF NOT EXISTS dim_seller
(
    seller_id String,
    seller_city String,
    seller_state String
)
ENGINE = MergeTree
ORDER BY seller_id;
