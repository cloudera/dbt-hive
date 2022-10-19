{% macro hive__array_construct(inputs, data_type) -%}
    array( {{ inputs|join(' , ') }} )
{%- endmacro %}
