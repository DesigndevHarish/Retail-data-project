SELECT

    product_id,

    product_name,

    product_brand AS brand,

    product_category AS category,

    SUM(quantity) AS total_units_sold,

    SUM(total_amount_inr) AS total_revenue,

    ROUND(
        AVG(unit_price_inr),
        2
    ) AS average_selling_price,

    count(event_id)as events_count

FROM {{ref("fact_retail_events")}}

WHERE event_type = 'SALE'

GROUP BY
    product_id,
    product_name,
    product_brand,
    product_category

ORDER BY total_revenue DESC