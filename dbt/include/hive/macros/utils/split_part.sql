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
{% macro hive__split_part(string_text, delimiter_text, part_number) %}

    {% set delimiter_expr %}

        -- escape if starts with a special character
        case when regexp_extract({{ delimiter_text }}, '([^A-Za-z0-9])(.*)', 1) != '_'
            then concat('\\', {{ delimiter_text }})
            else {{ delimiter_text }} end

    {% endset %}

    {% set split_part_expr %}

    split(
        {{ string_text }},
        {{ delimiter_expr }}
        )[({{ part_number - 1 }})]

    {% endset %}

    {{ return(split_part_expr) }}

{% endmacro %}
