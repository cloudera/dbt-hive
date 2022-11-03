{% macro assert_not_null(function, arg) %}
    coalesce({{function}}({{arg}}), nvl({{function}}({{arg}}), assert_true({{function}}({{arg}}) is not null), null))
{% endmacro %}

{% macro hive__datediff(first_date, second_date, datepart) %}

    {%- if datepart in ['day', 'week', 'month', 'quarter', 'year'] -%}

        {# make sure the dates are real, otherwise raise an error asap #}
        {% set first_date = assert_not_null('date', first_date) %}
        {% set second_date = assert_not_null('date', second_date) %}

    {%- endif -%}

    {%- if datepart == 'day' -%}

        datediff({{second_date}}, {{first_date}})

    {%- elif datepart == 'week' -%}

        case when {{first_date}} < {{second_date}}
            then floor(datediff({{second_date}}, {{first_date}})/7)
            else ceil(datediff({{second_date}}, {{first_date}})/7)
            end

        -- did we cross a week boundary (Sunday)?
        + case
            when {{first_date}} < {{second_date}} and dayofweek({{second_date}}) < dayofweek({{first_date}}) then 1
            when {{first_date}} > {{second_date}} and dayofweek({{second_date}}) > dayofweek({{first_date}}) then -1
            else 0 end

    {%- elif datepart == 'month' -%}

        case when {{first_date}} < {{second_date}}
            then floor(months_between(date({{second_date}}), date({{first_date}})))
            else ceil(months_between(date({{second_date}}), date({{first_date}})))
            end

        -- did we cross a month boundary?
        + case
            when {{first_date}} < {{second_date}} and dayofmonth({{second_date}}) < dayofmonth({{first_date}}) then 1
            when {{first_date}} > {{second_date}} and dayofmonth({{second_date}}) > dayofmonth({{first_date}}) then -1
            else 0 end

    {%- elif datepart == 'quarter' -%}

        case when {{first_date}} < {{second_date}}
            then floor(months_between(date({{second_date}}), date({{first_date}}))/3)
            else ceil(months_between(date({{second_date}}), date({{first_date}}))/3)
            end

        -- did we cross a quarter boundary?
        + case
            when {{first_date}} < {{second_date}} and (
                (weekofyear({{second_date}})*7 - (quarter({{second_date}}) * 365/4))
                < (weekofyear({{first_date}})*7 - (quarter({{first_date}}) * 365/4))
            ) then 1
            when {{first_date}} > {{second_date}} and (
                (weekofyear({{second_date}})*7 - (quarter({{second_date}}) * 365/4))
                > (weekofyear({{first_date}})*7 - (quarter({{first_date}}) * 365/4))
            ) then -1
            else 0 end

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
                {# make sure the timestamps are real, otherwise raise an error asap #}
                {{ assert_not_null('unix_timestamp', second_date) }}
                - {{ assert_not_null('unix_timestamp', first_date) }}
            ) / {{divisor}})
            else floor((
                {{ assert_not_null('unix_timestamp', second_date) }}
                - {{ assert_not_null('unix_timestamp', first_date) }}
            ) / {{divisor}})
            end

    {%- else -%}

        {{ exceptions.raise_compiler_error("macro datediff not implemented for datepart ~ '" ~ datepart ~ "' ~ on Spark") }}

    {%- endif -%}

{% endmacro %}