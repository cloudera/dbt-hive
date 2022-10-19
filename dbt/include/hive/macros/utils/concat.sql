{% macro hive__concat(fields) -%}
    concat({{ fields|join(', ') }})
{%- endmacro %}
