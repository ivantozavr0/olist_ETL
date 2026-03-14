CREATE TABLE IF NOT EXISTS dim_product
(
    product_id String,
    category String DEFAULT ''
)
ENGINE = MergeTree
ORDER BY product_id;
