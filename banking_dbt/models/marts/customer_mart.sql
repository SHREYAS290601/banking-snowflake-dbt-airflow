{{ config(materialized='table') }}

with source_customer_data as(
    SELECT
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    load_timestamp,
    dbt_valid_from as effective_from,
    dbt_valid_to as effective_to,
    CASE 
        WHEN dbt_valid_to is NULL THEN  False ELSE TRUE
    END as is_current

    FROM {{ ref('customers_snapshot') }}
)

SELECT * from source_customer_data