{% macro hive__array_construct(inputs, data_type) -%}
    {% if inputs|length > 0 %}
       {% if data_type == 'string' %}
          array( {{ '\"' + inputs|join('\", \"') + '\"' }} )
       {% else %}
          array( {{ inputs|join(', ')  }} )
       {% endif %}
    {% else %}
       {% if data_type == 'string' %}
          array("")
       {% else %}
          array()
       {% endif %}
    {% endif %}
{%- endmacro %}
