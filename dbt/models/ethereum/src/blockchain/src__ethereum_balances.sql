with source as (

    select * from {{ source('public_bigquery', 'ethereum_balances') }}

),

rename as (

    select
        -- PRIMARY DIMENSIONS
        address                             as ethereum_address,

        -- ADDITIONAL ATTRIBUTES
        eth_balance / 1000000000000000000   as ethereum_balance

    from source

)

select * from rename