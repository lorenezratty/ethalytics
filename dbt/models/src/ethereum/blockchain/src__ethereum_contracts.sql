with source as (

    select * from {{ source('public_bigquery', 'ethereum_contracts') }}

),

rename as (

    select
        -- PRIMARY DIMENSIONS
        block_hash                                          as block_hash,
        block_number                                        as block_number,
        address                                             as ethereum_address,

        -- ADDITIONAL ATTRIBUTES
        bytecode                                            as bytecode,
        function_sighashes                                  as function_sighashes,
        is_erc20                                            as is_erc20,
        is_erc721                                           as is_erc721,

        -- FOREIGN KEYS
        datetime(block_timestamp, 'America/Los_Angeles')   as block_created_at

    from source

)

select * from rename
