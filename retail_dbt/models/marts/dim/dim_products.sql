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

where product_id is not null

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY product_id,product_name,product_brand
    ORDER BY event_timestamp DESC
) = 1