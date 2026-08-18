select
    date_trunc(
        'Day',EVENT_TIMESTAMP
    ) as sales_date,

    count(distinct event_id)as total_sales,
    count(distinct customer_id)as unique_customers,
    sum(quantity) as units_sold,
    sum(quantity * unit_price_inr) as total_revenue_inr,
    round((avg(unit_price_inr)),2) as average_unit_price_inr

from {{ref("fact_retail_events")}}

where event_type = 'SALE'

group by 
     date_trunc(
        'Day',EVENT_TIMESTAMP
    )

order by sales_date

