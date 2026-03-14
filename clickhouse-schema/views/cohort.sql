CREATE MATERIALIZED VIEW IF NOT EXISTS olist.mv_cohort
ENGINE = MergeTree()
ORDER BY (month, months_passed)
AS
with tmp as
(
	select 
	fo.customer_unique_id,
	dateTrunc('month', first_value(fo.order_purchase_timestamp) 
			over (partition by fo.customer_unique_id order by fo.order_purchase_timestamp)) as month,
	dateDiff('months', first_value(fo.order_purchase_timestamp) 
		over (partition by fo.customer_unique_id order by fo.order_purchase_timestamp),
		fo.order_purchase_timestamp) as months_passed
	from olist.fact_orders fo
),
cohort as (
	select 
	month, 
	months_passed,
	countDistinct(customer_unique_id) as active_users
	from tmp
	group by month, months_passed
)
select 
c.month,
c.months_passed,
c.active_users,
c2.active_users as cohort_size,
c.active_users / c2.active_users * 100 as retention_rate
from cohort c
left join cohort c2 USING (month)
where c2.months_passed = 0
order by month, months_passed
