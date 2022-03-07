with source as (

    select * from {{ source('public_bigquery', 'ethereum_traces') }}

),

rename as (

    select
        transaction_hash,
        transaction_index,
        from_address,
        to_address,
        value * 0.000000000000000001 as value,
        input,
        output,
        trace_type,
        call_type,
        reward_type,
        gas,
        gas_used,
        subtraces,
        trace_address,
        error,
        status,
        block_timestamp,
        block_number,
        block_hash,
        trace_id

    from source

)

select * from rename