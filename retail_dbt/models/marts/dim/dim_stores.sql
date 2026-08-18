{{ config(
    materialized = 'view'
) }}
SELECT
    store_id,
    store_name,
    store_city,
    store_state,
    store_country

FROM {{ ref('stg_retail_events') }}

WHERE store_id IS NOT NULL

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY store_id
    ORDER BY event_timestamp DESC
) = 1