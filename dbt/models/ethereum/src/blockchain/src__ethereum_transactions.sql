with source as (

    select * from {{ source('public_bigquery', 'ethereum_transactions') }}

),

rename as (

    select
        -- PRIMARY DIMENSIONS
        source.hash                                         as transaction_hash,
        transaction_index                                   as transaction_index,

        -- ADDITIONAL ATTRIBUTES
        gas / 1000000000000000000                           as gas,
        gas_price / 1000000000000000000                     as gas_price,
        input                                               as input,
        max_fee_per_gas / 1000000000000000000               as max_fee_per_gas,
        max_priority_fee_per_gas / 1000000000000000000      as max_priority_fee_per_gas,
        nonce                                               as nonce,
        receipt_contract_address                            as receipt_contract_ethereum_address,
        receipt_cumulative_gas_used / 1000000000000000000   as receipt_cumulative_gas_used,
        receipt_effective_gas_price / 1000000000000000000   as receipt_effective_gas_price,
        receipt_gas_used / 1000000000000000000              as receipt_gas_used,
        receipt_root                                        as receipt_root,
        receipt_status                                      as receipt_status,
        transaction_type                                    as transaction_type,
        value / 1000000000000000000                         as transaction_value,
        
        -- FOREIGN KEYS
        block_hash                                          as block_hash,
        block_number                                        as block_number,
        from_address                                        as from_ethereum_address,
        to_address                                          as to_ethereum_address,

        -- METADATA
        datetime(block_timestamp, 'America/Chicago')        as created_at

    from source

)

select * from rename