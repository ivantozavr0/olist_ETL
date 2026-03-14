CREATE MATERIALIZED VIEW IF NOT EXISTS olist.mv_sales_daily
ENGINE = MergeTree()
ORDER BY (day)
AS
with items_per_order as (
    select 
    order_id,
    count(*) as items_numb
    from olist.fact_order_items
    group by order_id
)
select 
toDate(order_purchase_timestamp) as day,
count(fo.order_id) as total_orders,
countIf(order_id, order_status='delivered') as orders_delivered,
sumIf(payment_value, order_status='delivered') as total_payments,
avgIf(payment_value, order_status='delivered') as avg_payments,
avgIf(review_score, review_score IS NOT NULL) as avg_review,
sum(ipo.items_numb) / count(fo.order_id) as items_per_order
from olist.fact_orders fo
left join items_per_order ipo
on fo.order_id = ipo.order_id
group by toDate(order_purchase_timestamp);
