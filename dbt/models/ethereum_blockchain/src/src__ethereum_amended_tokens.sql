with source as (

    select * from {{ source('public_bigquery', 'ethereum_amended_tokens') }}

),

rename as (

    select
        address,
        symbol,
        decimals

    from source

)

select * from rename