select
        id as payment_id,
        orderid,
        paymentmethod,
        status,
        amount,
        created,
        _batched_at

    from {{ source('jaffle_shop', 'payment') }}

   --dbt-training-487516.jaffle_shop.payment