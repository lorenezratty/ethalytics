with source as (

    select * from {{ source('public_bigquery', 'ethereum_traces') }}

),

rename as (

    select
        transaction_hash,
        transaction_index,
        from_address                            as from_ethereum_address,
        to_address                              as to_ethereum_address,
        value,
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