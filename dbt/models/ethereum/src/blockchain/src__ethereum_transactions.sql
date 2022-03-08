with source as (

    select * from {{ source('public_bigquery', 'ethereum_transactions') }}

),

rename as (

    select
        -- hash,
        nonce,
        transaction_index,
        from_address                            as from_ethereum_address,
        to_address                              as to_ethereum_address,
        value / 1000000000000000000             as transaction_value,
        gas,
        gas_price,
        input,
        receipt_cumulative_gas_used,
        receipt_gas_used,
        receipt_contract_address                as receipt_contract_ethereum_address,
        receipt_root,
        receipt_status,
        block_timestamp,
        block_number,
        block_hash,
        max_fee_per_gas,
        max_priority_fee_per_gas,
        transaction_type,
        receipt_effective_gas_price

    from source

)

select * from rename