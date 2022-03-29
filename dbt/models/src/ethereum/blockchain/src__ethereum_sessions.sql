with source as (

    select * from {{ source('public_bigquery', 'ethereum_sessions') }}

),

rename as (

    select
        -- PRIMARY DIMENSIONS
        id                                                      as session_id,

        -- ADDITIONAL ATTRIBUTES
        start_block_number                                      as start_block_number,

        -- FOREIGN KEYS
        contract_address                                        as contract_address,
        wallet_address                                          as ethereum_address,
        start_trace_id                                          as trace_id,

        -- METADATA
        datetime(start_block_timestamp, 'America/Los_Angeles')  as start_block_timestamp

    from source

)

select * from rename
