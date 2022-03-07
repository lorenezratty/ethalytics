with source as (

    select * from {{ source('public_bigquery', 'ethereum_amended_tokens') }}

),

rename as (

    select
        address     as ethereum_address,
        symbol,
        decimals

    from source

)

select * from rename