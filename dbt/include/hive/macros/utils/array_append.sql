{% macro hive__array_append(array, new_element) -%}
    -- concat_ws only works with strings and string arrays
    split(concat_ws(',', {{ array }}, '{{ new_element }}'), ',')
{%- endmacro %}
