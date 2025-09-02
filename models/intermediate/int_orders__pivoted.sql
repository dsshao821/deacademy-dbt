with payments as 
(
    select * 
    from {{ ref('stg_stripe__payments') }}
    where status = 'success'
)
, pivoted as 
(
    select 
        order_id
        {%- set payment_methods = ['bank_transfer','coupon','credit_card','gift_card'] -%}
        {%- for payment_method in payment_methods %}
        , SUM(CASE WHEN payment_method = '{{ payment_method }}' THEN amount ELSE 0 END) AS {{ payment_method }}_amount
        {%- endfor %}

    from payments
    group by 1
)
select *
from pivoted