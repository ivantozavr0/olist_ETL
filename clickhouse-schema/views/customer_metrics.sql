CREATE MATERIALIZED VIEW IF NOT EXISTS olist.mv_customer_metrics
ENGINE = MergeTree()
ORDER BY (customer_unique_id)
AS
with tmp as (
	select 
	customer_unique_id, 
	avg(sellers_per_order) as sellers_per_order
	from
	(
		select 
		foi.customer_unique_id,
		countDistinct(foi.seller_id) as sellers_per_order
		from olist.fact_order_items foi
		group by foi.customer_unique_id, foi.order_id 
	) sub
	group by customer_unique_id
),
tmp1 as (
	select 
	foi.customer_unique_id,
	count(foi.order_item_id) as total_items,
	count(foi.order_item_id) / countDistinct(foi.order_id) as items_per_order,
	countDistinct(foi.seller_id) as total_sellers
	from olist.fact_order_items foi 
	group by foi.customer_unique_id
),
tmp2 as (
	SELECT 
	fo.customer_unique_id,
	COUNT(fo.order_id) as total_orders,
	sum(fo.payment_value) as total_spent,
	avg(fo.payment_value) as avg_order_value,
	min(fo.order_purchase_timestamp) as first_purchase_date,
	max(fo.order_purchase_timestamp) as last_purchase_date,
	dateDiff('days', min(fo.order_purchase_timestamp), max(fo.order_purchase_timestamp)) as lifetime_days,
	avg(fo.review_score) as avg_review_score
	FROM olist.fact_orders fo 
	group by customer_unique_id
)
select 
*
from tmp2 t2
left join tmp1 t1 using (customer_unique_id)
left join tmp t using (customer_unique_id)
















