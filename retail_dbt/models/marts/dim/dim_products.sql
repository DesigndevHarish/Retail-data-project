{{ config(
    materialized = 'view'
) }}

SELECT
    product_id,
    product_name AS name,
    product_brand AS brand,
    product_category AS category,
    availability_status AS availability,
    current_quantity AS available_quantity,
    unit_price_inr AS price_per_unit

FROM {{ ref("stg_retail_events") }}

WHERE product_id IS NOT NULL

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY product_id
    ORDER BY event_timestamp DESC
) = 1