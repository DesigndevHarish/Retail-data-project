with source_data as (
    select 
        Event_data,
        Source_file_name,
        Load_timestamp
    from {{source('bronze','BRONZE_RETAIL_EVENTS')}}
),

staged_data as 
(
    select
        event_data:event_id::varchar as event_id,
        event_data:event_timestamp::timestamp_ntz as event_timestamp,
        event_data:event_type::varchar as event_type,

        event_data:customer.customer_id::varchar as customer_id,
        event_data:customer.customer_city::varchar as customer_city,
        event_data:customer.customer_segment::varchar as customer_segment,

        event_data:product.product_id::varchar  as product_id,
        case
         when product_id is null then 'UNKNOWN_PRODUCT'
         else  'KNOWN_PRODUCT'
        end as product_flag,
        event_data:product.product_name::varchar  as product_name,
        event_data:product.brand::varchar  as product_brand,
        event_data:product.category::varchar  as product_category,
        
        event_data:store.store_id::varchar as store_id,
        event_data:store.store_name::varchar as store_name,
        event_data:store.city::varchar as store_city,
        event_data:store.state::varchar as store_state,
        event_data:store.country::varchar as store_country,

        event_data:inventory.availability::varchar as availability_status,
        event_data:inventory.current_quantity::number as current_quantity,
        event_data:inventory.previous_quantity::number as prev_quantity,
        event_data:inventory.quantity_change::number as change_of_quantity,


        event_data:transaction.currency::varchar as currency,
        event_data:transaction.payment_method::varchar as payment_method,
        event_data:transaction.quantity::number as quantity,
        event_data:transaction.unit_price_inr::number(14,2) as unit_price_inr,

        Source_file_name,
        Load_timestamp
        

        from source_data

)

select * from staged_data