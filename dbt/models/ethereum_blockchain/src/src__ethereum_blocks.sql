with source as (

    select * from {{ source('public_bigquery', 'ethereum_blocks') }}

),

rename as (

    select
        timestamp,
        number,
        -- hash,
        parent_hash,
        nonce,
        sha3_uncles,
        logs_bloom,
        transactions_root,
        state_root,
        receipts_root,
        miner,
        difficulty,
        total_difficulty,
        size,
        extra_data,
        gas_limit,
        gas_used,
        transaction_count,
        base_fee_per_gas

    from source

)

select * from rename