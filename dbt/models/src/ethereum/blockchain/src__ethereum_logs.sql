with source as (

    select * from {{ source('public_bigquery', 'ethereum_logs') }}

),

rename as (

    select
        -- PRIMARY DIMENSIONS
        log_index                                           as log_index,

        -- ADDITIONAL ATTRIBUTES
        data                                                as data,
        topics                                              as topics,
        transaction_index                                   as transaction_index,

        -- FOREIGN KEYS
        block_hash                                          as block_hash,
        block_number                                        as block_number,
        address                                             as ethereum_address,
        transaction_hash                                    as transaction_hash,

        -- METADATA
        datetime(block_timestamp, 'America/Los_Angeles')    as created_at_pt

    from source

)

select * from rename
