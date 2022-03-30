{{
    config(
        materialized='incremental',
        unique_key='transaction_hash',
        partition_by={
          "field": "created_at_pt",
          "data_type": "timestamp",
          "granularity": "day"
        }
    )
}}

with

transactions as (

    select
        *,
        block_hash || from_ethereum_address as from_surrogate_transaction_key,
        block_hash || to_ethereum_address   as to_surrogate_transaction_key

    from {{ ref('src__ethereum_transactions') }}

    {% if is_incremental() %}

    where created_at_pt > (select max(created_at_pt) from {{ this }})

    {% endif %}

),

contracts as (

    select
        surrogate_transaction_key,
        sum(case when is_erc20 then 1 else 0 end)   as num_erc20_contracts,
        sum(case when is_erc721 then 1 else 0 end)  as num_erc721_contracts

    from {{ ref('prep__ethereum_contracts') }}

    group by 1

),

ethereum_balances as (

    select
        ethereum_address,
        balance_in_eth      as current_balance_in_eth

    from {{ ref('src__ethereum_balances') }}

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
        gas_used / gas_limit                        as perc_gas_limit_used,
        receiving_contracts.num_erc20_contracts     as receiving_num_erc20_contracts,
        receiving_contracts.num_erc721_contracts    as receiving_num_erc721_contracts,
        root,
        sending_contracts.num_erc20_contracts       as sending_num_erc20_contracts,
        sending_contracts.num_erc721_contracts      as sending_num_erc721_contracts,
        status,
        gas_price_in_eth * gas_used                 as transaction_fee_in_eth,
        gas_price_in_gwei * gas_used                as transaction_fee_in_gwei,
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
        created_at_pt

    from transactions

    left join ethereum_balances as receiving_address_balances
        on transactions.to_ethereum_address = receiving_address_balances.ethereum_address
    left join ethereum_balances as sending_address_balances
        on transactions.from_ethereum_address = sending_address_balances.ethereum_address

    left join contracts as receiving_contracts
        on transactions.to_surrogate_transaction_key = receiving_contracts.surrogate_transaction_key
    left join contracts as sending_contracts
        on transactions.from_surrogate_transaction_key = sending_contracts.surrogate_transaction_key

)

select * from final
