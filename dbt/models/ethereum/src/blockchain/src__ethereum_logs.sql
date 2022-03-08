with source as (

    select * from {{ source('public_bigquery', 'ethereum_logs') }}

),

rename as (

    select
        log_index,
        transaction_hash,
        transaction_index,
        address                                         as ethereum_address,
        data,
        topics,
        datetime(block_timestamp, 'America/Chicago')    as created_at,
        block_number                                    as created_on_block,
        block_hash                                      as created_on_hash

    from source

)

select * from rename