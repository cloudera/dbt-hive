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

{% macro hive__date_trunc(datepart, date) -%}
    {% set datepart_lower = datepart.lower() %}
    {%- if datepart_lower == 'microseconds' -%}
        date_format({{date}}, 'yyyy-MM-dd HH:mm:ss.SSSSSS')
    {%- elif datepart_lower == 'milliseconds' -%}
        date_format({{date}}, 'yyyy-MM-dd HH:mm:ss.SSS')
    {%- elif datepart_lower == 'seconds' -%}
        date_format({{date}}, 'yyyy-MM-dd HH:mm:ss')
    {%- elif datepart_lower == 'minutes' -%}
        date_format({{date}}, 'yyyy-MM-dd HH:mm')
    {%- elif datepart_lower == 'hours' -%}
        date_format({{date}}, 'yyyy-MM-dd HH:00')
    {%- elif datepart_lower == 'day' -%}
        date_format({{date}}, 'yyyy-MM-dd')
    {%- elif datepart_lower == 'month' -%}
        date_format({{date}}, 'yyyy-MM-01')
    {%- elif datepart_lower == 'year' -%}
        date_format({{date}}, 'yyyy-01-01')
    {%- else -%}
        {{ exceptions.raise_compiler_error("macro date_format not implemented for datepart ~ '" ~ datepart ~ "' ~ on Hive") }}
    {%- endif -%}
{%- endmacro %}
