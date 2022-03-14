with source as (

    select * from {{ source('public_bigquery', 'ethereum_contracts') }}

),

rename as (

    select
        address                                         as ethereum_address,
        bytecode,
        function_sighashes,
        is_erc20,
        is_erc721,
        datetime(block_timestamp, 'America/Chicago')    as created_at,
        block_number                                    as block_number,
        block_hash                                      as block_hash

    from source

)

select * from rename