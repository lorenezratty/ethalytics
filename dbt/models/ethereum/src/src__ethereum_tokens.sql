with source as (

    select * from {{ source('public_bigquery', 'ethereum_tokens') }}

),

rename as (

    select
        address             as ethereum_address,
        symbol,
        name,
        decimals,
        total_supply,
        block_timestamp,
        block_number,
        block_hash

    from source

)

select * from rename