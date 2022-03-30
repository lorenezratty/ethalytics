{#
Hey there, a friendly comment here to let you know that BigQuery has billable limits. This model
is specifically very large, there are tons of blocks out there! If you're running into a bug
"500 Query exceeded limit for bytes billed: 1000000000000" then I recommend you head over to your profiles
setup. From here, you will need to increase the maximum_bytes_billed. If you are on the free tier you
should have enough credits to get this to startup. If you are billing yourself or off the trial credits
BE AWARE you could accidentally stack up an enormous bill. Have fun!
#}

{{
    config(
        materialized='incremental',
        unique_key='block_hash',
        partition_by={
          "field": "created_at_pt",
          "data_type": "timestamp",
          "granularity": "day"
        }
    )
}}

with source as (

    select * from {{ source('public_bigquery', 'ethereum_blocks') }}

),

rename as (

    select
        -- PRIMARY DIMENSIONS
        source.hash                                 as block_hash,
        number                                      as block_number,

        -- ADDITIONAL ATTRIBUTES
        base_fee_per_gas                            as base_fee_per_gas,
        difficulty / 1000000000000                  as difficulty_in_th,
        extra_data                                  as extra_data,
        gas_limit / 1000000000000000000             as gas_limit,
        gas_used / 1000000000000000000              as gas_used,
        logs_bloom                                  as logs_bloom,
        nonce                                       as nonce,
        sha3_uncles                                 as sha3_uncles,
        size                                        as size_in_bytes,
        state_root                                  as state_root,
        receipts_root                               as receipts_root,
        transaction_count                           as transaction_count,
        transactions_root                           as transactions_root,
        total_difficulty / 1000000000000            as total_difficulty_in_th,

        -- FOREIGN KEYS
        miner                                       as miner_ethereum_address,
        parent_hash                                 as parent_hash,

        -- METADATA
        datetime(timestamp, 'America/Los_Angeles')  as created_at_pt

    from source

)

select * from rename

{% if is_incremental() %}

where created_at_pt > (select max(created_at_pt) from {{ this }})

{% endif %}

