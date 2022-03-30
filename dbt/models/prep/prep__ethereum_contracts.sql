with

contracts as (

    select * from {{ ref('src__ethereum_contracts') }}

),

final as (

    select
        -- PRIMARY DIMENSIONS
        contracts.block_hash            as block_hash,
        contracts.block_number          as block_number,

        -- ADDITIONAL ATTRIBUTES
        contracts.bytecode              as bytecode,
        contracts.function_sighashes    as function_sighashes,
        contracts.is_erc20              as is_erc20,
        contracts.is_erc721             as is_erc721,

        -- FOREIGN KEYS
        contracts.ethereum_address      as ethereum_address,
        block_hash || ethereum_address  as surrogate_transaction_key,

        -- METADATA
        contracts.created_at_pt         as created_at_pt

    from contracts

)

select * from final
