{% macro hive__array_concat(array_1, array_2) -%}
    concat_ws(',', {{ array_1 }}, {{ array_2 }})
{%- endmacro %}
