with source as (

    select * from {{ ref('known_eth_addresses') }}

),

rename as (

    select
        id          as known_address_id,
        address     as ethereum_address,
        name        as address_owner_name

    from source

)

select * from rename