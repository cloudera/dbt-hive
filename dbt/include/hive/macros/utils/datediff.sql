{% macro hive__datediff(first_date, second_date, datepart) %}

    {%- if datepart == 'day' -%}

        datediff({{second_date}}, {{first_date}})

    {%- elif datepart == 'week' -%}

        case when {{first_date}} < {{second_date}}
            then
                ceil( datediff({{second_date}}, {{first_date}}) / 7 )
            else 
                floor( datediff({{second_date}}, {{first_date}}) / 7 )
            end

    {%- elif datepart == 'month' -%}

        case when {{first_date}} < {{second_date}}
            then
                ceil(months_between({{second_date}}, {{first_date}}))
            else
                floor(months_between({{second_date}}, {{first_date}}))
            end

    {%- elif datepart == 'quarter' -%}

        case when {{first_date}} < {{second_date}}
            then
                ceil( months_between({{second_date}}, {{first_date}}) / 3 )
            else
                floor( months_between({{second_date}}, {{first_date}}) / 3 )
            end

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