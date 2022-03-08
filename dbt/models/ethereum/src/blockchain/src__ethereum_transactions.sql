with source as (

    select * from {{ source('public_bigquery', 'ethereum_transactions') }}

),

rename as (

    select
        -- hash,
        nonce,
        transaction_index,
        from_address                                        as from_ethereum_address,
        to_address                                          as to_ethereum_address,
        value / 1000000000000000000                         as transaction_value,
        gas / 1000000000000000000                           as gas,
        gas_price / 1000000000000000000                     as gas_price,
        input,
        receipt_cumulative_gas_used / 1000000000000000000   as receipt_cumulative_gas_used,
        receipt_gas_used / 1000000000000000000              as receipt_gas_used,
        receipt_contract_address                            as receipt_contract_ethereum_address,
        receipt_root,
        receipt_status,
        datetime(block_timestamp, 'America/Chicago')        as created_at,
        block_number                                        as created_on_block,
        block_hash                                          as created_on_hash,
        max_fee_per_gas / 1000000000000000000               as max_fee_per_gas,
        max_priority_fee_per_gas / 1000000000000000000      as max_priority_fee_per_gas,
        transaction_type,
        receipt_effective_gas_price / 1000000000000000000   as receipt_effective_gas_price

    from source

)

select * from rename