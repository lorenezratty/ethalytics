with

transactions as (

    select * from {{ ref('src__ethereum_transactions') }}

),

surrogate_keys as (

    select
        transaction_hash,
        {{ dbt_utils.surrogate_key(['block_hash', 'from_ethereum_address']) }}  as from_surrogate_transaction_key,
        {{ dbt_utils.surrogate_key(['block_hash', 'to_ethereum_address']) }}    as to_surrogate_transaction_key

    from transactions

),

final as (

    select
        -- PRIMARY KEYS
        transaction_hash,
        transaction_index,

        -- ADDITIONAL ATTRIBUTES
        gas_limit,
        gas_in_gwei,
        gas_price_in_gwei,
        gas_price_in_eth,
        input,
        max_fee_per_gas_in_gwei,
        max_priority_fee_per_gas_in_gwei,
        nonce,
        contract_ethereum_address,
        cumulative_gas_used,
        effective_gas_price,
        gas_used,
        gas_used / gas_limit                as perc_gas_limit_used,
        root,
        status,
        gas_price_in_eth * gas_used         as transaction_fee_in_eth,
        gas_price_in_gwei * gas_used        as transaction_fee_in_gwei,
        transaction_type,
        transaction_value_in_eth,

        -- FOREIGN KEYS
        block_hash,
        block_number,
        from_ethereum_address,
        to_ethereum_address,
        from_surrogate_transaction_key,
        to_surrogate_transaction_key,

        -- METADATA
        created_at

    from transactions
    left join surrogate_keys using (transaction_hash)

)

select * from final
