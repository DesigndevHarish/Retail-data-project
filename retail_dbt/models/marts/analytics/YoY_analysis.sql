with current_sales as (
    select
         sales_month_name as month, 
         sales_year as year, 
         sales_month as num, 
         sum(total_revenue_inr) as total_revenue

    from {{ref("fact_sale_analysis")}}

        group by
            month,
            year,
            num
),

yoy_analysis as(
    select
        month,
        year,
        num,
        total_revenue,
        lag(total_revenue,12) over(order by year,num) as prev_year_revenue,
        round(
            ((total_revenue - prev_year_revenue)/nullif(prev_year_revenue,0))*100,2
        ) as "GROWTH%",
        case
            when "GROWTH%" >= 0 THEN 'INCREMENTAL_TREND'
            when "GROWTH%" < 0 THEN 'DECREMENTAL_TREND'
            ELSE 'NO_PREV_DATA'
        end as growth_trend


    from current_sales
)

select * from yoy_analysis order by year desc , num asc