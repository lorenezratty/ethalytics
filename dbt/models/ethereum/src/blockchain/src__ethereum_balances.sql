with source as (

    select * from {{ source('public_bigquery', 'ethereum_balances') }}

),

rename as (

    select
        address                             as ethereum_address,
        eth_balance / 1000000000000000000   as ethereum_balance

    from source

)

select * from rename