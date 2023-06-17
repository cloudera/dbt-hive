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
