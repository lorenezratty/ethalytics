with

blocks as (

    select * from {{ ref('src__ethereum_blocks') }}
    order by block_created_at desc

),

tokens as (

    select * from {{ ref('prep__ethereum_tokens') }}

),

contracts as (

    select * from {{ ref('src__ethereum_contracts') }}

),

logs as (

    select * from {{ ref('src__ethereum_logs') }}

),

token_transfers as (

    select * from {{ ref('src__ethereum_token_transfers') }}

),

traces as (

    select * from {{ ref('src__ethereum_traces') }}

),

transactions as (

    select * from {{ ref('prep__ethereum_transactions') }}

),

traces_agg as (

    select
        block_hash,
        coalesce(sum(gas_limit), 0)               as sum_gas,
        coalesce(sum(gas_used), 0)          as sum_gas_used,
        coalesce(sum(value_transferred), 0) as sum_trace_value_transferred

    from traces

    group by 1

),

tokens_agg as (

    select
        block_hash,
        coalesce(count(*), 0) as num_tokens_created_on_block

    from tokens

    group by 1

),

transactions_agg as (

    select
        block_hash,
        coalesce(sum(gas), 0)                           as sum_transaction_gas,
        coalesce(min(gas_price), 0)                     as min_transaction_gas_price,
        coalesce(max(gas_price), 0)                     as max_transacrion_gas_price,
        coalesce(max(max_fee_per_gas), 0)               as max_fee_per_gas,
        coalesce(max(max_priority_fee_per_gas), 0)      as max_priority_fee_per_gas,
        coalesce(sum(cumulative_gas_used), 0)           as sum_transaction_cumulative_gas_used,
        coalesce(min(effective_gas_price), 0)           as min_effective_gas_price,
        coalesce(max(effective_gas_price), 0)           as max_effective_gas_price,
        coalesce(sum(gas_used), 0)                      as sum_transaction_gas_used,
        coalesce(sum(transaction_value_in_eth), 0)      as sum_transaction_value_in_eth

    from transactions

    group by 1

),

contracts_agg as (

    select
        block_hash,

        coalesce(
            count(
                case
                when is_erc20 = true
                then 1
                else 0
                end
            ),
        0)                              as num_erc_20,

        coalesce(
            count(
                case
                when is_erc721 = true
                then 1
                else 0
                end
            ),
        0)                              as num_erc_721,

        coalesce(count(*), 0)           as num_contracts

    from contracts

    group by 1

),

logs_agg as (

    select
        block_hash,
        coalesce(count(*), 0) as num_logs

    from logs

    group by 1

),

token_transfers_agg as (

    select
        block_hash,
        coalesce(count(*), 0)               as num_token_transfers,
        coalesce(sum(value_in_ethereum), 0) as sum_value_in_ethereum

    from token_transfers

    group by 1

),

final as (

    select
        block_hash,
        block_number,
        base_fee_per_gas,
        difficulty,
        extra_data,
        gas_limit,
        gas_used,
        logs_bloom,
        nonce,
        sha3_uncles,
        size_in_bytes,
        state_root,
        
        sum_gas,
        sum_gas_used,
        sum_trace_value_transferred,
        num_tokens_created_on_block,
        sum_transaction_gas,
        min_transaction_gas_price,
        max_transacrion_gas_price,
        max_fee_per_gas,
        max_priority_fee_per_gas,
        sum_transaction_receipt_cumulative_gas_used,
        min_receipt_effective_gas_price,
        max_receipt_effective_gas_price,
        sum_transaction_receipt_gas_used,
        sum_transaction_value,
        num_erc_20,
        num_erc_721,
        num_contracts,
        num_logs,
        -- num_token_transfers,
        -- sum_value_in_ethereum,

        receipts_root,
        transaction_count,
        transactions_root,
        total_difficulty,
        miner_ethereum_address,
        parent_hash,
        block_created_at

    from blocks
    left join traces_agg            using (block_hash)
    left join tokens_agg            using (block_hash)
    left join transactions_agg      using (block_hash)
    left join contracts_agg         using (block_hash)
    left join logs_agg              using (block_hash)
    left join token_transfers_agg   using (block_hash)

    order by block_created_at

)

select * from final