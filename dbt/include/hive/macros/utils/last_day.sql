{%- macro default_last_day(date, datepart) -%}
    {% if datepart == 'quarter' %}
        cast(
            {{
                dbt.dateadd('day', '-1', 
                    dbt.dateadd('month', '3', 
                        dbt.date_trunc('month', date)
                    )
                )
            }}
            as date)
    {% else %}
        cast(
            {{
                dbt.dateadd('day', '-1', 
                    dbt.dateadd(datepart, '1', 
                        dbt.date_trunc(datepart, date)
                    )
                )
            }}
            as date)
    {% endif %}
{%- endmacro -%}