with source as (

    select * from {{ source('public_bigquery', 'ethereum_balances') }}

),

rename as (

    select
        address,
        eth_balance

    from source

)

select * from rename