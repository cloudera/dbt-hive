{% macro hive__right(string_text, length_expression) %}

    case when {{ length_expression }} = 0
        then NULL
    else
        substr(
        {{ string_text }},
        (length({{ string_text }})-cast({{ length_expression }} as int)+1),
        length({{ string_text }})-1
    )
    end
    
{%- endmacro -%}