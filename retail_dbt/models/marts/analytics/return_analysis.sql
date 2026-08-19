SELECT
    product_id,
    product_name,
    product_brand AS brand,
    product_category AS category,

    SUM(quantity) AS total_returned_units,

    SUM(total_amount_inr) AS total_return_value,

    COUNT(event_id) AS return_event_count

FROM {{ref("fact_retail_events")}}

WHERE event_type = 'RETURN'

GROUP BY
    product_id,
    product_name,
    product_brand,
    product_category

ORDER BY total_return_value DESC