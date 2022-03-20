{{
    config(
        materialized='incremental',
        unique_key='block_hash'
    )
}}

select * from {{ ref('prep__ethereum_blocks') }}

{% if is_incremental() %}

  where block_created_at > (select max(block_created_at) from {{ this }})

{% endif %}