with monthly_report as (
    select
        sales_month_name as month,
        sales_month as month_num,
        sales_year as year,
        sum(total_sales)as sales_count,
        sum(total_units_sold)as units_sold,
        sum(total_revenue_inr) as total_revenue

    from {{ref("fact_sale_analysis")}}

    group by 
    month, year ,month_num
     
),

sales_trend_analysis as (
    select * ,
    lag(total_revenue) over (order by year , month_num) as prev_month_revenue,
    round(((total_revenue - prev_month_revenue)/nullif(prev_month_revenue,0))*100,2)  as "MOM%",
    case
        when "MOM%" >=10 then 'Strong_growth'
        when "MOM%" >=0 then 'Stable_growth'
        else 'Declining'
    end as sales_trend

    from monthly_report

)

select * from sales_trend_analysis order by year desc, month_num asc