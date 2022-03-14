with source as (

    select * from {{ source('public_bigquery', 'ethereum_tokens') }}

),

rename as (

    select
        -- PRIMARY DIMENSIONS
        name                                            as token_name,
        symbol                                          as token_symbol,
        
        -- ADDITIONAL ATTRIBUTES
        decimals                                        as num_token_decimals,
        total_supply                                    as total_supply,

        -- FOREIGN KEYS
        address                                         as ethereum_address,
        block_hash                                      as block_hash,

        -- METADATA
        block_number                                    as block_number,
        datetime(block_timestamp, 'America/Chicago')    as created_at

    from source

)

select * from rename