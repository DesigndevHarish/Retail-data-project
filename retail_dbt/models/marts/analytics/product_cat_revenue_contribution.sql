with product_details as (
    select
        product_category,
        sum(quantity) as total_units_sold,
        sum(total_amount_inr)as total_revenue

    from {{ref("fact_retail_events")}}
    where event_type='SALE'
    group by product_category
    order by total_revenue  desc
),

revenue_contribution as (
    select 
        product_category,
        total_units_sold,
        total_revenue,
        round(
            div0null(total_revenue,sum(total_revenue)over())*100,2
        ) as revenue_percenctage,
        dense_rank()over(order by total_revenue desc) as rank

        from product_details
)

select * from revenue_contribution