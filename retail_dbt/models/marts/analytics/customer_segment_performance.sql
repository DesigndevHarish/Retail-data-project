SELECT 
    customer_segment AS segment,

    COUNT(DISTINCT customer_id) AS total_customers,

    COUNT(event_id) AS total_orders,

    SUM(quantity) AS total_units_sold,

    SUM(total_amount_inr) AS total_revenue,

    ROUND(
        DIV0NULL(
            SUM(total_amount_inr),
            COUNT(event_id)
        ),
        2
    ) AS average_order_value

FROM {{ref("fact_retail_events")}}

WHERE event_type = 'SALE'

GROUP BY customer_segment

ORDER BY total_revenue DESC