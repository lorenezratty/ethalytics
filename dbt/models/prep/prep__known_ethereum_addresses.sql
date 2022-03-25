with

known_addresses as (

    select * from {{ ref('src__known_ethereum_addresses') }}

),

balances as (

    select * from {{ ref('src__ethereum_balances') }}

),

receiving_transactions as (

    select
        to_ethereum_address             as ethereum_address,
        count(*)                        as num_of_receiving_transactions,
        sum(transaction_value_in_eth)   as total_value_received

    from {{ ref('src__ethereum_transactions') }}

    group by 1

),

sending_transactions as (

    select
        from_ethereum_address           as ethereum_address,
        count(*)                        as num_of_sent_transactions,
        sum(transaction_value_in_eth)   as total_value_sent

    from {{ ref('src__ethereum_transactions') }}

    group by 1

),


final as (

    select
        known_addresses.*,
        balances.balance_in_eth as current_balance_in_ethereum,
        receiving_transactions.num_of_receiving_transactions,
        receiving_transactions.total_value_received,
        sending_transactions.num_of_sent_transactions,
        sending_transactions.total_value_sent

    from known_addresses
    left join balances using (ethereum_address)
    left join sending_transactions using (ethereum_address)
    left join receiving_transactions using (ethereum_address)

    order by 1

)

select * from final
