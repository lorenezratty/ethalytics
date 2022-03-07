with source as (

    select * from {{ source('public_bigquery', 'ethereum_sessions') }}

),

rename as (

    select
        id,
        start_trace_id,
        start_block_number,
        start_block_timestamp,
        wallet_address,
        contract_address

    from source

)

select * from rename