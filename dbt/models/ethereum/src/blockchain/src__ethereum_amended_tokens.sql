with source as (

    select * from {{ source('public_bigquery', 'ethereum_amended_tokens') }}

),

rename as (

    select
        address     as ethereum_address,
        symbol      as token_symbol,
        decimals    as num_token_decimals

    from source

)

select * from rename