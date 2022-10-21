{% macro hive__hash(expression) -%}
    case when {{ expression }} = NULL
        then md5('')
    else
        md5({{ expression }})
    end
{%- endmacro %}