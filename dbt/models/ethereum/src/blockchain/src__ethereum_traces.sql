with source as (

    select * from {{ source('public_bigquery', 'ethereum_traces') }}

),

rename as (

    select
        transaction_hash,
        transaction_index,
        from_address                                    as from_ethereum_address,
        to_address                                      as to_ethereum_address,
        value / 1000000000000000000                     as value_transferred,
        input,
        output,
        trace_type,
        call_type,
        reward_type,
        gas / 1000000000000000000                       as gas,
        gas_used / 1000000000000000000                  as gas_used,
        subtraces,
        trace_address,
        error,
        status,
        datetime(block_timestamp, 'America/Chicago')    as created_at,
        block_number                                    as created_on_block,
        block_hash                                      as created_on_hash,
        trace_id

    from source

)

select * from rename