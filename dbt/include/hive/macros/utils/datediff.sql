{#
# Copyright 2022 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#}

{% macro hive__datediff(first_date, second_date, datepart) %}

    {%- if datepart == 'day' -%}

        datediff({{second_date}}, {{first_date}})

    {%- elif datepart == 'week' -%}

        case when {{first_date}} < {{second_date}}
            then floor( datediff({{second_date}}, {{first_date}}) / 7 )
            else ceil( datediff({{second_date}}, {{first_date}}) / 7 )
            end

        -- did we cross a week boundary (Sunday)
        + case
            when {{first_date}} < {{second_date}} and dayofweek(cast({{second_date}} as timestamp)) < dayofweek(cast({{first_date}} as timestamp)) then 1
            when {{first_date}} > {{second_date}} and dayofweek(cast({{second_date}} as timestamp)) > dayofweek(cast({{first_date}} as timestamp)) then -1
            else 0 end

    {%- elif datepart == 'month' -%}

        case when {{first_date}} < {{second_date}}
            then floor(months_between({{second_date}}, {{first_date}}))
            else ceil(months_between({{second_date}}, {{first_date}}))
            end

        -- did we cross a month boundary?
        + case
            when {{first_date}} < {{second_date}} and dayofmonth(cast({{second_date}} as timestamp)) < dayofmonth(cast({{first_date}} as timestamp)) then 1
            when {{first_date}} > {{second_date}} and dayofmonth(cast({{second_date}} as timestamp)) > dayofmonth(cast({{first_date}} as timestamp)) then -1
            else 0 end

    {%- elif datepart == 'quarter' -%}

        ((year({{second_date}}) - year({{first_date}})) * 4 + quarter({{second_date}}) - quarter({{first_date}}))

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

        {{ exceptions.raise_compiler_error("macro datediff not implemented for datepart ~ '" ~ datepart ~ "' ~ on Hive") }}

    {%- endif -%}

{% endmacro %}
