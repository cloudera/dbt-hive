{% macro hive__get_empty_subquery_sql(select_sql, select_sql_header=none) %}
    {%- if select_sql_header is not none -%}
    {{ select_sql_header }}
    {%- endif -%}
    select * from (
        {{ select_sql }}
    ) as dbt_sbq
    where false
    limit 0
{% endmacro %}

{% macro hive__get_empty_schema_sql(columns) %}
    {%- set col_err = [] -%}
    select
    {%- for i in columns %}
      {%- set col = columns[i] -%}
      {%- if col['data_type'] is not defined -%}
        {{ col_err.append(col['name']) }}
      {%- else -%}
        {%- set col_name = adapter.quote(col['name']) if col.get('quote') else col['name'] -%}
        {%- set dtype = col['data_type'] | lower -%}

        {# To represent "null" for complex types #}
        {%- if dtype.startswith('array') -%}
          {%- set null_expr = "array(null)" -%}
        {%- elif dtype.startswith('map') -%}
          {%- set null_expr = "map(null, null)" -%}
        {%- else -%}
          {%- set null_expr = "null" -%}
        {%- endif -%}

        {{ "  " }}cast({{ null_expr }} as {{ dtype }}) as {{ col_name }}
        {%- if not loop.last -%},{{ "\n" }}{%- endif -%}
      {%- endif -%}
    {%- endfor %}

    {%- if (col_err | length) > 0 -%}
      {{ exceptions.column_type_missing(column_names=col_err) }}
    {%- endif -%}
{% endmacro %}
