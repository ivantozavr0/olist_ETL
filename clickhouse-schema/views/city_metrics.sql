CREATE MATERIALIZED VIEW IF NOT EXISTS olist.mv_city_metrics
ENGINE = MergeTree()
ORDER BY (city)
AS
with tmp as (
	select 
	seller_city as city,
	seller_state as state,
	count(ds.seller_id) as sellers_in_city
	from olist.dim_seller ds 
	group by ds.seller_city, ds.seller_state 
),
main_struct as (
	select 
	fo.customer_city as city,
	fo.customer_state as state,
	count(fo.order_id) as total_orders,
	sumIf(payment_value, order_status='delivered') as total_payments,
	avgIf(payment_value, order_status='delivered') as avg_payments,
	countDistinct(fo.customer_unique_id) as total_customers,
	avg(fo.review_score) as avg_review_score,
	avg(dd.delivery_days) as avg_delivery_days,
	median(dd.delivery_days) as median_delivery_days,
	countIf(fo.order_id, dd.order_delivered_customer_date > dd.order_estimated_delivery_date)
			/ count(fo.order_id) * 100 as late_delivery_rate
	from 
	olist.fact_orders fo 
	left join olist.dim_delivery dd using (order_id)
	group by fo.customer_city, fo.customer_state
)
select *
from main_struct ms
left join tmp t using (city, state)
