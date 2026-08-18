{{ config(
    materialized = 'view'
) }}

WITH customer_data AS (

    SELECT
        CUSTOMER_ID,
        CUSTOMER_CITY,
        CUSTOMER_SEGMENT,
        EVENT_TIMESTAMP

    FROM {{ ref('stg_retail_events') }}

    WHERE CUSTOMER_ID IS NOT NULL

),

deduplicated AS (

    SELECT
        CUSTOMER_ID,
        CUSTOMER_CITY,
        CUSTOMER_SEGMENT,

        ROW_NUMBER() OVER (
            PARTITION BY CUSTOMER_ID
            ORDER BY EVENT_TIMESTAMP DESC
        ) AS rn

    FROM customer_data

)

SELECT
    SHA2(CUSTOMER_ID, 256) AS CUSTOMER_KEY,
    CUSTOMER_ID,
    CUSTOMER_CITY,
    CUSTOMER_SEGMENT

FROM deduplicated

WHERE rn = 1