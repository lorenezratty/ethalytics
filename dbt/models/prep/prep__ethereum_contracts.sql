with

contracts as (

    select * from {{ ref('src__ethereum_contracts') }}

),

final as (

    select
        contracts.block_hash,
        contracts.block_number,
        contracts.ethereum_address,
        contracts.bytecode,
        contracts.function_sighashes,
        contracts.is_erc20,
        contracts.is_erc721,
        contracts.block_created_at,
        {{ dbt_utils.surrogate_key(['block_hash', 'ethereum_address']) }} as surrogate_transaction_key

    from contracts

)

select * from final
