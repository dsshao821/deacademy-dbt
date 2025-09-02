{% macro clean_stale_models(database=target.database, schema=target.schema, days=7, dry_run = true) %}

    {% set get_drop_commands_query %}
    select 
          CASE WHEN table_type = 'VIEW' then table_type ELSE 'TABLE' end as drop_type 
        , 'DROP ' || drop_type || ' {{ database | upper }}.' || table_schema || '.' || table_name || ';' as drop_query
    from {{ database }}.information_schema.tables
    where table_schema = upper('{{ schema }}')
      and date(last_altered) <= date(current_date()- {{days}})

    {% endset %}

    {{ log('\nGenerating cleanup queries...\n', info=True) }}
    {% set drop_queries = run_query(get_drop_commands_query).columns[1].values() %}

    {% for query in drop_queries %}
        {% if dry_run %}
            {{ log(query, info=true)}}
        {% else %}
            {{ log('Dropping object with command: ' ~ query, info=true)}}
            {% do run_query(query)%}
        {% endif %}
        
    {% endfor %}

{% endmacro %}