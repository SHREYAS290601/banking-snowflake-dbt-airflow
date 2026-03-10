{{ config(materialized='incremental', unique_key='transaction_id') }}

SELECT
    t.transaction_id,
    t.account_id,
    a.customer_id,
    t.amount,
    t.related_account_id,
    t.status,
    t.transaction_type,
    t.transaction_time,
    CURRENT_TIMESTAMP as load_timestamp
FROM {{ ref('transactions_st') }} as t
LEFT JOIN {{ ref('accounts_st') }} as a
    ON t.account_id = a.account_id
WHERE 1=1
{% if is_incremental() %}
  AND t.transaction_time > (SELECT MAX(transaction_time) FROM {{ this }})
{% endif %}