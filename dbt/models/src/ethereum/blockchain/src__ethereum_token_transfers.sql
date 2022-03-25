with source as (

    select * from {{ source('public_bigquery', 'ethereum_token_transfers') }}

),

rename as (

    select
        -- PRIMARY DIMENSIONS
        token_address                                           as token_ethereum_address,
        from_address                                            as from_ethereum_address,
        to_address                                              as to_ethereum_address,

        -- ADDITIONAL ATTRIBUTES
        log_index                                               as log_index,
        transaction_hash                                        as transaction_hash,
        cast(trim(value) as bignumeric) / 1000000000000000000   as value_in_ethereum,

        -- FOREIGN KEYS
        block_hash                                              as block_hash,
        block_number                                            as block_number,

        -- METADATA
        datetime(block_timestamp, 'America/Los_Angeles')       as created_at

    from source

)

select * from rename
