with source as (

    select * from {{ source('public_bigquery', 'ethereum_traces') }}

),

rename as (

    select
        -- PRIMARY DIMENSIONS
        trace_id                                        as trace_id,

        -- ADDITIONAL ATTRIBUTES
        call_type                                       as call_type,
        error                                           as error,
        gas / 1000000000000000000                       as gas_limit,
        gas_used / 1000000000000000000                  as gas_used,
        input                                           as input,
        output                                          as output,
        reward_type                                     as reward_type,
        status                                          as status,
        subtraces                                       as subtraces,
        trace_address                                   as trace_address,
        trace_type                                      as trace_type,
        value / 1000000000000000000                     as value_transferred,

        -- FOREIGN KEYS
        block_number                                    as block_number,
        block_hash                                      as block_hash,
        from_address                                    as from_ethereum_address,
        transaction_hash                                as transaction_hash,
        transaction_index                               as transaction_index,
        to_address                                      as to_ethereum_address,


        -- METADATA
        datetime(block_timestamp, 'America/Chicago')    as block_created_at

    from source

)

select * from rename