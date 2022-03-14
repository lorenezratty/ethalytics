with source as (

    select * from {{ source('public_bigquery', 'ethereum_blocks') }}

),

rename as (

    select
        -- PRIMARY DIMENSIONS
        source.hash                             as block_hash,
        number                                  as block_number,

        -- ADDITIONAL ATTRIBUTES
        base_fee_per_gas                        as base_fee_per_gas,
        difficulty                              as difficulty,
        extra_data                              as extra_data,
        gas_limit / 1000000000000000000         as gas_limit,
        gas_used / 1000000000000000000          as gas_used,
        logs_bloom                              as logs_bloom,
        nonce                                   as nonce,
        sha3_uncles                             as sha3_uncles,
        size                                    as size_in_bytes,
        state_root                              as state_root,
        receipts_root                           as receipts_root,
        transaction_count                       as transaction_count,
        transactions_root                       as transactions_root,
        total_difficulty                        as total_difficulty,

        -- FOREIGN KEYS
        miner                                   as miner_ethereum_address,
        parent_hash                             as parent_hash,


        -- METADATA
        datetime(timestamp, 'America/Chicago')  as timestamp

    from source

)

select * from rename