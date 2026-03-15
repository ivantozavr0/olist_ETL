CREATE MATERIALIZED VIEW IF NOT EXISTS olist.mv_seller_metrics
ENGINE = MergeTree()
ORDER BY (seller_id)
AS
with reviews as (
    select 
    fo.order_id,
    avg(review_score) as review_score
    from olist.dim_reviews fo
    left join olist.fact_order_items foi using (order_id)
    group by fo.order_id
    having (count(*) = 1) or (countDistinct(foi.seller_id) = 1)
)
select 
seller_id,
count(order_item_id) as items_ordered,
countIf(order_item_id, foi.order_status = 'delivered') as items_sold,
sumIf(price, foi.order_status = 'delivered') as total_revenue,
avgIf(price, foi.order_status = 'delivered') as avg_revenue,
avg(review_score) as avg_review_score,
100.0 * countIf(foi.order_status = 'canceled') 
                / nullIf(count(), 0) as cancellation_rate,
avg(dd.delivery_days) as avg_delivery_days
from olist.fact_order_items foi 
left join reviews using (order_id)
left join olist.dim_delivery dd using (order_id)
group by seller_id;
