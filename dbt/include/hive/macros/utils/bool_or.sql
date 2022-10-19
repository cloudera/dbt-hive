{% hive impala__bool_or(expression) -%}
    bit_or({{ expression }})
{%- endmacro %}
