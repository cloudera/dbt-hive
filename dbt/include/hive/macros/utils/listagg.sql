{% macro hive__listagg(measure, delimiter_text, order_by_clause, limit_num) -%}

  {% if order_by_clause %}
    {{ exceptions.warn("order_by_clause is not supported for listagg on Hive") }}
  {% endif %}

  {% if limit_num %}
    {{ exceptions.warn("limit_num is not supported for listagg on Hive") }}
  {% endif %}

  {% set collected_list %} collect_list({{ measure }}) {% endset %}

  {% set final %} concat_ws({{ delimiter_text }}, {{ collected_list }}) {% endset %}

  {% do return(final) %}

{%- endmacro %}
