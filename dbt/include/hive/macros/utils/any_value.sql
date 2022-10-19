{% macro hive__any_value(expression) -%}
    {#-- return any value (non-deterministic)  --#}
    first_value({{ expression }}) over()
{%- endmacro %}
