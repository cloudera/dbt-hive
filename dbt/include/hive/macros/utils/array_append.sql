{% macro hive__array_append(array, new_element) -%}
    concat_ws(',', {{ array }}, {{ new_element }})
{%- endmacro %}
