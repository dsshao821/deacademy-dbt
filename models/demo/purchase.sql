{{
    config
    (
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'PURCHASE_ID',
        merge_exclude_columns = ['INSERT_DTS']
    )
}}

with purchase_src as
(
    select
    PURCHASE_ID, PURCHASE_DATE, PURCHASE_STATUS, CREATED_AT
    , CURRENT_TIMESTAMP AS INSERT_DTS
    , current_timestamp as update_dts
    from {{source('purchase','PURCHASE_SRC')}}

    {% if is_incremental() %}
    where CREATED_AT > (SELECT MAX(INSERT_DTS) FROM {{this}})
    {% endif %}
)
select * from purchase_src