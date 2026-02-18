{{
    config(
        materialized='view'
    )
}}

with customers as (

    select * from {{ref('stg_customers')}}
),

orders as (

    select * from {{ref('stg_orders')}}
),

payment as (

    select * from {{ref('stg_payment')}}
),

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from orders

    group by 1

),

customer_payments as (
    select o.customer_id, sum(p.amount) as total_payment_amount, count(p.payment_id) as number_of_payments
    from payment as p
    left join orders as o
    on p.orderid = o.order_id
    group by 1
),

final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        coalesce(customer_payments.total_payment_amount, 0) as total_payment_amount,
        coalesce(customer_payments.number_of_payments, 0) as number_of_payments


    from customers

    left join customer_orders using (customer_id)
    left join customer_payments using (customer_id)

)

select * from final