with source as (

    select * from {{ source('public_bigquery', 'ethereum_token_transfers') }}

),

rename as (

    select
        token_address                                   as token_ethereum_address,
        from_address                                    as from_ethereum_address,
        to_address                                      as to_ethereum_address,
        cast(value as numeric) / 1000000000000000000    as value_in_ethereum,
        transaction_hash,
        log_index,
        datetime(block_timestamp, 'America/Chicago')    as created_at,
        block_number                                    as created_on_block,
        block_hash                                      as created_on_hash

    from source

)

select * from rename