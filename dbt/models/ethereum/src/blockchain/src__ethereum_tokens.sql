with source as (

    select * from {{ source('public_bigquery', 'ethereum_tokens') }}

),

rename as (

    select
        address                                         as ethereum_address,
        symbol                                          as token_symbol,
        name                                            as token_name,
        decimals                                        as num_token_decimals,
        total_supply,
        datetime(block_timestamp, 'America/Chicago')    as created_at,
        block_number                                    as created_on_block,
        block_hash                                      as created_on_hash

    from source

)

select * from rename