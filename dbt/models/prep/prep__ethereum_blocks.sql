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
        coalesce(avg(gas_limit), 0)         as avg_gas_limit,
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
        coalesce(count(*) ,0)                               as num_transactions,
        coalesce(avg(gas_in_gwei), 0)                       as avg_transaction_gas_in_gwei,
        coalesce(max(gas_in_gwei), 0)                       as max_transaction_gas_in_gwei,
        coalesce(min(gas_in_gwei), 0)                       as min_transaction_gas_in_gwei,
        coalesce(min(gas_price_in_eth), 0)                  as min_transaction_gas_price_in_eth,
        coalesce(max(gas_price_in_eth), 0)                  as max_transacrion_gas_price_in_eth,
        coalesce(min(gas_price_in_gwei), 0)                 as min_transaction_gas_price_in_gwei,
        coalesce(max(gas_price_in_gwei), 0)                 as max_transacrion_gas_price_in_gwei,
        coalesce(max(max_fee_per_gas_in_gwei), 0)           as max_fee_per_gas_in_gwei,
        coalesce(max(max_priority_fee_per_gas_in_gwei), 0)  as max_priority_fee_per_gas_in_gwei,
        coalesce(sum(cumulative_gas_used), 0)               as sum_transaction_cumulative_gas_used,
        coalesce(min(effective_gas_price), 0)               as min_effective_gas_price,
        coalesce(max(effective_gas_price), 0)               as max_effective_gas_price,
        coalesce(sum(gas_used), 0)                          as sum_transaction_gas_used,
        coalesce(sum(transaction_value_in_eth), 0)          as sum_transaction_value_in_eth

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
        -- PRIMARY DIMENSIONS
        blocks.block_hash                                       as block_hash,
        blocks.block_number                                     as block_number,

        -- ADDITIONAL ATTRIBUTES
        traces_agg.avg_gas_limit                                as avg_gas_limit,
        transactions_agg.avg_transaction_gas_in_gwei            as avg_transaction_gas_in_gwei,
        blocks.base_fee_per_gas                                 as base_fee_per_gas,
        blocks.difficulty_in_th                                 as difficulty_in_th,
        blocks.extra_data                                       as extra_data,
        blocks.gas_limit                                        as gas_limit,
        blocks.gas_used                                         as gas_used,
        blocks.logs_bloom                                       as logs_bloom,
        transactions_agg.max_effective_gas_price                as max_effective_gas_price,
        transactions_agg.max_fee_per_gas_in_gwei                as max_fee_per_gas_in_gwei,
        transactions_agg.max_priority_fee_per_gas_in_gwei       as max_priority_fee_per_gas_in_gwei,
        transactions_agg.max_transaction_gas_in_gwei            as max_transaction_gas_in_gwei,
        transactions_agg.max_transacrion_gas_price_in_eth       as max_transacrion_gas_price_in_eth,
        transactions_agg.max_transacrion_gas_price_in_gwei      as max_transacrion_gas_price_in_gwei,
        transactions_agg.min_effective_gas_price                as min_effective_gas_price,
        transactions_agg.min_transaction_gas_in_gwei            as min_transaction_gas_in_gwei,
        transactions_agg.min_transaction_gas_price_in_eth       as min_transaction_gas_price_in_eth,
        transactions_agg.min_transaction_gas_price_in_gwei      as min_transaction_gas_price_in_gwei,
        blocks.nonce                                            as nonce,
        contracts_agg.num_contracts                             as num_contracts,
        contracts_agg.num_erc_20                                as num_erc_20,
        contracts_agg.num_erc_721                               as num_erc_721,
        logs_agg.num_logs                                       as num_logs,
        token_transfers_agg.num_token_transfers                 as num_token_transfers,
        tokens_agg.num_tokens_created_on_block                  as num_tokens_created_on_block,
        transactions_agg.num_transactions                       as num_transactions,
        blocks.sha3_uncles                                      as sha3_uncles,
        blocks.receipts_root                                    as receipts_root,
        blocks.size_in_bytes                                    as size_in_bytes,
        blocks.state_root                                       as state_root,
        blocks.total_difficulty_in_th                           as total_difficulty_in_th,
        blocks.transaction_count                                as transaction_count,
        blocks.transactions_root                                as transactions_root,
        traces_agg.sum_gas_used                                 as sum_gas_used,
        traces_agg.sum_trace_value_transferred                  as sum_trace_value_transferred,
        transactions_agg.sum_transaction_cumulative_gas_used    as sum_transaction_cumulative_gas_used,
        transactions_agg.sum_transaction_gas_used               as sum_transaction_gas_used,
        transactions_agg.sum_transaction_value_in_eth           as sum_transaction_value_in_eth,
        token_transfers_agg.sum_value_in_ethereum               as sum_value_in_ethereum,

        -- FOREIGN KEYS
        blocks.miner_ethereum_address                           as miner_ethereum_address,
        blocks.parent_hash                                      as parent_hash,

        -- METADATA
        blocks.block_created_at                                 as created_at

    from blocks
    left join traces_agg            using (block_hash)
    left join tokens_agg            using (block_hash)
    left join transactions_agg      using (block_hash)
    left join contracts_agg         using (block_hash)
    left join logs_agg              using (block_hash)
    left join token_transfers_agg   using (block_hash)

)

select * from final
