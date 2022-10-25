{% macro hive__escape_single_quotes(expression) -%}
{{ expression | replace("'","\\'") }}
{%- endmacro %}