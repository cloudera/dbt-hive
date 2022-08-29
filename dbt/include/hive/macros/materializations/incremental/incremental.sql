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

{% materialization incremental, adapter='hive' -%}
  
  {#-- Validate early so we don't run SQL if the file_format + strategy combo is invalid --#}
  {%- set raw_file_format = config.get('file_format', default='parquet') -%}
  {%- set raw_strategy = config.get('incremental_strategy', default='append') -%}
  
  {%- set file_format = dbt_hive_validate_get_file_format(raw_file_format) -%}
  {%- set strategy = dbt_hive_validate_get_incremental_strategy(raw_strategy, file_format) -%}
  
  {%- set unique_key = config.get('unique_key', none) -%}
  {%- set partition_by = config.get('partition_by', none) -%}

  {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}

  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}

  {% if strategy == 'insert_overwrite' and partition_by %}
    {% call statement() %}
        -- SET hive.exec.dynamic.partition=true
        -- SET hive.exec.dynamic.partition.mode=nonstrict
        SELECT 1
    {% endcall %}
  {% endif %}

  {{ run_hooks(pre_hooks) }}

  {% set drop_temp_relation = False %}

  {% if existing_relation is none %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif existing_relation.is_view or full_refresh_mode %}
    {% do adapter.drop_relation(existing_relation) %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% else %}
    {{ drop_relation(tmp_relation) }}  {# call the drop_relation macro directy instead of the dbt-core method to avoid type check, as type is null for tmp_relation #}
    {% do run_query(create_table_as(False, tmp_relation, sql)) %}
    {% set drop_temp_relation = True %}
    {% set build_sql = dbt_hive_get_incremental_sql(strategy, tmp_relation, target_relation, unique_key) %}
  {% endif %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

  {{ run_hooks(post_hooks) }}

  {% if drop_temp_relation %}
    {{ drop_relation(tmp_relation) }}  {# call the drop_relation macro directy instead of the dbt-core method to avoid type check, as type is null for tmp_relation #}
  {% endif %}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
