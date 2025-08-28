{{
    config
    (
        materialized = 'table'
    )
}}

select * from {{ ref('project2_raw') }} where name in ('abc','xyz')