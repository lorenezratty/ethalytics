with source as (

    select * from {{ source('public_bigquery', 'ethereum_contracts') }}

),

rename as (

    select
        address,
        bytecode,
        function_sighashes,
        is_erc20,
        is_erc721,
        block_timestamp,
        block_number,
        block_hash

    from source

)

select * from rename