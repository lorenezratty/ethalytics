with source as (

    select * from {{ source('public_bigquery', 'ethereum_transactions') }}

),

rename as (

    select
        -- PRIMARY DIMENSIONS
        source.hash                                         as transaction_hash,
        transaction_index                                   as transaction_index,

        -- ADDITIONAL ATTRIBUTES
        gas                                                 as gas_limit,
        gas / 1000000000000000000                           as gas_in_gwei,
        gas_price * 0.000000001                             as gas_price_in_gwei,
        gas_price * 0.000000000000000001                    as gas_price_in_eth,
        input                                               as input,
        max_fee_per_gas / 1000000000000000000               as max_fee_per_gas_in_gwei,
        max_priority_fee_per_gas / 1000000000000000000      as max_priority_fee_per_gas_in_gwei,
        nonce                                               as nonce,
        receipt_contract_address                            as contract_ethereum_address,
        receipt_cumulative_gas_used                         as cumulative_gas_used,
        receipt_effective_gas_price                         as effective_gas_price,
        receipt_gas_used                                    as gas_used,
        receipt_root                                        as root,
        receipt_status                                      as status,
        transaction_type                                    as transaction_type,
        value / 1000000000000000000                         as transaction_value_in_eth,

        -- FOREIGN KEYS
        block_hash                                          as block_hash,
        block_number                                        as block_number,
        from_address                                        as from_ethereum_address,
        to_address                                          as to_ethereum_address,

        -- METADATA
        datetime(block_timestamp, 'American/Los Angeles')   as created_at

    from source

)

select * from rename
