with source as (

    select * from {{ source('public_bigquery', 'ethereum_logs') }}

),

rename as (

    select
        log_index,
        transaction_hash,
        transaction_index,
        address             as ethereum_address,
        data,
        topics,
        block_timestamp,
        block_number,
        block_hash

    from source

)

select * from rename