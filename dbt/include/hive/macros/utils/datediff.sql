{% macro hive__datediff(first_date, second_date, datepart) %}

    {%- if datepart == 'day' -%}

        datediff({{second_date}}, {{first_date}})

    {%- elif datepart == 'week' -%}

        weekofyear({{second_date}}) -  weekofyear({{first_date}})

    {%- elif datepart == 'month' -%}

        months_between({{second_date}}, {{first_date}})

    {%- elif datepart == 'quarter' -%}

        months_between({{second_date}}, {{first_date}}) / 3

    {%- elif datepart == 'year' -%}

        year({{second_date}}) - year({{first_date}})

    {%- elif datepart in ('hour', 'minute', 'second') -%}

        {%- set divisor -%}
            {%- if datepart == 'hour' -%} 3600
            {%- elif datepart == 'minute' -%} 60
            {%- elif datepart == 'second' -%} 1
            {%- endif -%}
        {%- endset -%}

        case when {{first_date}} < {{second_date}}
            then ceil((
                unix_timestamp( {{second_date}} ) - unix_timestamp( {{first_date}} ) 
            ) / {{divisor}})
            else floor((
                unix_timestamp( {{second_date}} ) - unix_timestamp( {{first_date}} ) 
            ) / {{divisor}})
            end

    {%- else -%}

        {{ exceptions.raise_compiler_error("macro datediff not implemented for datepart ~ '" ~ datepart ~ "' ~ on Spark") }}

    {%- endif -%}

{% endmacro %}