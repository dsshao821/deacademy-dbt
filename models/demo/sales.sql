{{
    config
    (
        materialized = 'incremental',
        incremental_strategy = 'append'
    )
}}

with sales_src as
(
    select
    SALE_ID
  , SALE_DATE
  , CUSTOMER_ID
  , PRODUCT_ID
  , QUANTITY
  , TOTAL_AMOUNT
  , CREATED_AT
  , CURRENT_TIMESTAMP AS INSERT_DTS
  from {{source('sales','SALES_SRC')}}

  {% if is_incremental() %}
  WHERE CREATED_AT > (SELECT MAX(INSERT_DTS) FROM {{this}})
  {% endif %}
)
select * from sales_src