
{{ config(
    materialized = 'incremental',
    unique_key = 'event_id',
    incremental_strategy = 'merge'
) }}

SELECT
    EVENT_ID,
    EVENT_TIMESTAMP,
    EVENT_TYPE,

    CUSTOMER_ID,
    CUSTOMER_CITY,
    CUSTOMER_SEGMENT,

    PRODUCT_ID,
    PRODUCT_NAME,
    PRODUCT_BRAND,
    PRODUCT_CATEGORY,

    STORE_ID,
    STORE_NAME,
    STORE_CITY,
    STORE_STATE,
    STORE_COUNTRY,

    AVAILABILITY_STATUS,
    PREV_QUANTITY,
    CHANGE_OF_QUANTITY,
    CURRENT_QUANTITY,

    QUANTITY,
    UNIT_PRICE_INR,

    QUANTITY * UNIT_PRICE_INR AS TOTAL_AMOUNT_INR,

    CURRENCY,
    PAYMENT_METHOD,

    SOURCE_FILE_NAME,
    LOAD_TIMESTAMP

FROM {{ ref('stg_retail_events') }}

WHERE EVENT_ID IS NOT NULL

{% if is_incremental() %}

    AND LOAD_TIMESTAMP > (
        SELECT COALESCE(
            MAX(LOAD_TIMESTAMP),
            '1900-01-01'::TIMESTAMP
        )
        FROM {{ this }}
    )

{% endif %}

