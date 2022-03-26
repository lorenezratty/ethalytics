{{
    config(
        materialized='incremental',
        unique_key='transaction_hash'
    )
}}

select * from {{ ref('prep__ethereum_transactions') }}

{% if is_incremental() %}

  where created_at > (select max(created_at) from {{ this }})

{% endif %}
