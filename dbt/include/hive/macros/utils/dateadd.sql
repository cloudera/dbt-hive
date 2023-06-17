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

{% macro hive__dateadd(datepart, interval, from_date_or_timestamp) %}
    {%- if datepart == 'day' -%}
        from_unixtime(unix_timestamp(date_add({{from_date_or_timestamp}}, cast({{interval}} as int)))+(cast(hour({{from_date_or_timestamp}}) as int)*3600) + (cast(minute({{from_date_or_timestamp}}) as int)*60) + (cast(second({{from_date_or_timestamp}}) as int)))
    {% elif datepart == 'month' -%}
        from_unixtime(unix_timestamp(add_months({{from_date_or_timestamp}}, cast({{interval}} as int),'YYYY-MM-dd HH:mm:ss')))
    {% elif datepart == 'year' -%}
        from_unixtime(unix_timestamp(add_months({{from_date_or_timestamp}}, (cast({{interval}} as int)*12),'YYYY-MM-dd HH:mm:ss')))
    {%- elif datepart == 'hour' -%}
        from_unixtime(unix_timestamp({{from_date_or_timestamp}}) + cast({{interval}} as int)*3600)
    {%- else -%}

        {{ exceptions.raise_compiler_error("macro datediff not implemented for datepart ~ '" ~ datepart ~ "' ~ on Spark") }}

    {%- endif -%}
{% endmacro %}
