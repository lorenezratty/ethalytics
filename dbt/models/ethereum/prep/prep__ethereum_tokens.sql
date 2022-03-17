with

amended_tokens as (

    select ethereum_address from {{ ref('src__ethereum_amended_tokens') }}

),

tokens as (

    select * from {{ ref('src__ethereum_tokens') }}

),

final as (

    select * from tokens
    left join amended_tokens using (ethereum_address)

)

select * from final