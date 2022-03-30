with source as (

    select * from {{ source('public_bigquery', 'ethereum_tokens') }}

),

rename as (

    select
        -- PRIMARY DIMENSIONS
        name                                                as token_name,
        symbol                                              as token_symbol,

        -- ADDITIONAL ATTRIBUTES
        decimals                                            as num_token_decimals,
        total_supply                                        as total_supply,

        -- FOREIGN KEYS
        block_hash                                          as block_hash,
        block_number                                        as block_number,
        address                                             as ethereum_address,

        -- METADATA
        datetime(block_timestamp, 'America/Los_Angeles')    as created_at_pt

    from source

)

select * from rename
