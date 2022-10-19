{% macro hive__date_trunc(datepart, date) -%}
    trunc({{date}}, '{{datepart}}')
{%- endmacro %}