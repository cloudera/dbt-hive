{% macro hive__bool_or(expression) -%}
    max({{ expression }})
{%- endmacro %}
