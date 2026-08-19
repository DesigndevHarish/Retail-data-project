WITH recent_filter AS (
    SELECT 
        product_id,
        product_name,
        product_brand,
        product_category,
        current_quantity,
        availability_status,
        store_id,
        store_name,
        store_city
    FROM fact_retail_events
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            product_id,
            store_id,
            product_name,
            product_brand,
            product_category
        ORDER BY event_timestamp DESC
    ) = 1
)

SELECT *
FROM recent_filter
WHERE availability_status IN (
    'OUT_OF_STOCK',
    'LOW_STOCK'
)