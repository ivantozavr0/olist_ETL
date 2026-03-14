CREATE MATERIALIZED VIEW IF NOT EXISTS olist.mv_product_metrics
ENGINE = MergeTree()
ORDER BY (product_id)
AS
with reviews as (
    select 
    fo.order_id,
    avg(review_score) as review_score
    from olist.fact_orders fo
    left join olist.fact_order_items foi using (order_id)
    group by fo.order_id
    having (count(*) = 1) or (countDistinct(foi.product_id) = 1)
),
main_struct as (
	select 
		foi.product_id,
		COUNT(*) as total_orders,
		countIf(foi.order_item_id, order_status='canceled') / COUNT(*) * 100 as cancelation_rate,
		sumIf(foi.price, order_status='delivered') as total_revenue,
		avg(foi.price) as avg_price,
		avg(r.review_score) as avg_review_score
	from olist.fact_order_items foi 
	left join reviews r using (order_id)
	group by foi.product_id
)
select *
from main_struct ms
left join olist.dim_product dp using (product_id)
