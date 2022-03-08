with source as (

    select * from {{ source('public_bigquery', 'ethereum_blocks') }}

),

rename as (

    select
        datetime(timestamp, 'America/Chicago')  as timestamp,
        number                                  as block_number,
        -- hash,
        parent_hash,
        nonce,
        sha3_uncles,
        logs_bloom,
        transactions_root,
        state_root,
        receipts_root,
        miner                                   as miner_ethereum_address,
        difficulty,
        total_difficulty,
        size                                    as size_in_bytes,
        extra_data,
        gas_limit / 1000000000000000000         as gas_limit,
        gas_used / 1000000000000000000          as gas_used,
        transaction_count,
        base_fee_per_gas

    from source

)

select * from rename