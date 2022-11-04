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

        {{ exceptions.raise_compiler_error("macro datediff not implemented for datepart ~ '" ~ datepart ~ "' ~ on Hive") }}

    {%- endif -%}

{% endmacro %}
