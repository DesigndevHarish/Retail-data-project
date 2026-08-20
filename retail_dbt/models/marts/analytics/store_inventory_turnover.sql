WITH sales AS (

    SELECT
        store_id,
        store_name,
        store_city,

        SUM(quantity) AS total_units_sold

    FROM fact_retail_events

    WHERE event_type = 'SALE'

    GROUP BY
        store_id,
        store_name,
        store_city
),

latest_inventory AS (

    SELECT
        product_id,
        product_name,
        product_brand,
        product_category,

        store_id,
        store_name,
        store_city,

        current_quantity

    FROM fact_retail_events

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            product_id,
            product_name,
            product_brand,
            product_category,
            store_id

        ORDER BY event_timestamp DESC
    ) = 1
),

store_inventory AS (

    SELECT
        store_id,
        store_name,
        store_city,

        SUM(current_quantity) AS current_inventory

    FROM latest_inventory

    GROUP BY
        store_id,
        store_name,
        store_city
)

SELECT
    s.store_id,
    s.store_name,
    s.store_city,

    s.total_units_sold,

    i.current_inventory,

    ROUND(
        DIV0NULL(
            s.total_units_sold,
            i.current_inventory
        ),
        2
    ) AS inventory_turnover_ratio,

    CASE
        WHEN DIV0NULL(
            s.total_units_sold,
            i.current_inventory
        ) >= 5
            THEN 'HIGH_TURNOVER'

        WHEN DIV0NULL(
            s.total_units_sold,
            i.current_inventory
        ) >= 2
            THEN 'NORMAL'

        ELSE 'LOW_TURNOVER'

    END AS inventory_status

FROM sales s

LEFT JOIN store_inventory i
    ON s.store_id = i.store_id

ORDER BY inventory_turnover_ratio DESC