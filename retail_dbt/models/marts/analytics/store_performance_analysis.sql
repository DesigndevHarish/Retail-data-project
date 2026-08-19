SELECT 
    store_id,
    store_name,
    store_city,
    store_state,
    store_country,

    SUM(quantity) AS total_units_sold,

    SUM(total_amount_inr) AS total_revenue,

    COUNT(event_id) AS total_sales_events,

    ROUND(
        DIV0NULL(
            SUM(total_amount_inr),
            COUNT(event_id)
        ),
        2
    ) AS average_order_value

FROM fact_retail_events

WHERE event_type = 'SALE'

GROUP BY 
    store_id,
    store_name,
    store_city,
    store_state,
    store_country

ORDER BY total_revenue DESC