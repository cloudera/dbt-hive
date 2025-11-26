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

{% macro dbt_hive_validate_get_file_format(raw_file_format) %}
  {#-- Validate the file format #}

  {% set accepted_formats = ['text', 'csv', 'json', 'jdbc', 'parquet', 'orc', 'hive', 'delta', 'libsvm', 'avro'] %}

  {% set invalid_file_format_msg -%}
    Invalid file format provided: {{ raw_file_format }}
    Expected one of: {{ accepted_formats | join(', ') }}
  {%- endset %}

  {% if raw_file_format not in accepted_formats %}
    {% do exceptions.raise_compiler_error(invalid_file_format_msg) %}
  {% endif %}

  {% do return(raw_file_format) %}
{% endmacro %}


{% macro dbt_hive_validate_get_incremental_strategy(raw_strategy) %}
  {#-- Validate the incremental strategy #}

  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ raw_strategy }}
    Expected one of: 'append', 'merge', 'insert_overwrite', 'microbatch'
  {%- endset %}

  {% set invalid_insert_overwrite_endpoint_msg -%}
    Invalid incremental strategy provided: {{ raw_strategy }}
    You cannot use this strategy when connecting via endpoint
    Use the 'append' or 'merge' strategy instead
  {%- endset %}

  {% if raw_strategy not in ['append', 'merge', 'insert_overwrite', 'microbatch'] %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {%-else %}
    {% if raw_strategy == 'insert_overwrite' and target.endpoint %}
      {% do exceptions.raise_compiler_error(invalid_insert_overwrite_endpoint_msg) %}
    {% endif %}
  {% endif %}

  {% if raw_strategy == 'microbatch' %}
      {{ validate_partition_key_for_microbatch_strategy() }}
  {%- endif -%}

  {% do return(raw_strategy) %}
{% endmacro %}

{% macro validate_partition_key_for_microbatch_strategy() %}
    {% set microbatch_partition_key_missing_msg -%}
      dbt-hive 'microbatch' incremental strategy requires a `partition_by` config.
      Ensure you are using a `partition_by` column that is of granularity {{ config.get('batch_size') }}.
    {%- endset %}

    {%- if not config.get('partition_by') -%}
      {{ exceptions.raise_compiler_error(microbatch_partition_key_missing_msg) }}
    {%- endif -%}
{% endmacro %}
