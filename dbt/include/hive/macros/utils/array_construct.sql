{% macro hive__array_construct(inputs, data_type) -%}
    {% if inputs|length > 0 %}
    array( {{ inputs|join(' , ') }} )
    {% else %}
    array()
    {% endif %}
{%- endmacro %}
