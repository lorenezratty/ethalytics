with source as (

    select * from {{ source('public_bigquery', 'ethereum_sessions') }}

),

rename as (

    select
        id,
        start_trace_id,
        start_block_number,
        datetime(start_block_timestamp, 'America/Chicago')  as start_block_timestamp,
        wallet_address                                      as ethereum_address,
        contract_address

    from source

)

select * from rename