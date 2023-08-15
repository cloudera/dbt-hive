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

  -- relations
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set temp_relation = make_temp_relation(target_relation)-%}
  {%- set intermediate_relation = make_intermediate_relation(target_relation)-%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}


  -- configs
  {%- set unique_key = config.get('unique_key') -%}
  {%- set full_refresh_mode = (should_full_refresh()  or existing_relation.is_view) -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}

  {#-- Validate early so we don't run SQL if the file_format + strategy combo is invalid --#}
  {%- set raw_file_format = config.get('file_format', default='parquet') -%}
  {%- set incremental_strategy = config.get('incremental_strategy', default='append') -%}
  {% if incremental_strategy == None %}
      {% set incremental_strategy = 'append' %}
  {% endif %}

  {% do target_relation.log_relation(incremental_strategy) %}

  -- fetch version to check if the strategy is supported
  {% set hive_version = get_hive_version() | int %}
  {{ log("HIVE VERSION " ~ hive_version) }}

  {#-- support merge in only Hive 3 --#}
  {% if (hive_version < 3 and incremental_strategy == 'merge') %}
    {% do exceptions.raise_compiler_error("incremental_strategy='merge' is only supported in Hive 3") %}
    {{ return(None) }}
  {% endif %}

  {#-- For non-partition tables insert overwrite is not supported --#}
  {%- set partitioned_by = config.get('partition_by') -%}
  {%- if incremental_strategy == 'insert_overwrite' -%}
    {%- if partitioned_by is none -%}
      {%- do exceptions.raise_compiler_error("insert_overwrite strategy is not supported for non-partition tables") -%}
      {{ return(None) }}
    {%- endif -%}
    {% set table_type = config.get('table_type') %}
    {%- if table_type == 'iceberg' -%}
      {%- do exceptions.raise_compiler_error("insert_overwrite strategy is not supported for iceberg tables") -%}
      {{ return(None) }}
    {%- endif -%}
  {%- endif -%}


  -- the temp_ and backup_ relations should not already exist in the database; get_relation
  -- will return None in that case. Otherwise, we get a relation that we can drop
  -- later, before we try to use this name for the current operation. This has to happen before
  -- BEGIN, in a separate transaction
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation)-%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
   -- grab current tables grants config for comparision later on
  {% set grant_config = config.get('grants') %}
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}

  {% if existing_relation is none %}
      {% set build_sql = get_create_table_as_sql(False, target_relation, sql) %}
  {% elif full_refresh_mode %}
      {% set build_sql = get_create_table_as_sql(False, intermediate_relation, sql) %}
      {% set need_swap = true %}
  {% else %}
    {{ drop_relation(temp_relation) }}
    {% do run_query(get_create_table_as_sql(False, temp_relation, sql)) %}
    {% do to_drop.append(temp_relation) %}
    {% do adapter.expand_target_column_types(
             from_relation=temp_relation,
             to_relation=target_relation) %}
    {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
    {% set dest_columns = process_schema_changes(on_schema_change, temp_relation, existing_relation) %}
    {% if not dest_columns %}
      {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% endif %}

    {#-- Get the incremental_strategy, the macro to use for the strategy, and build the sql --#}
    {% set build_sql = dbt_hive_get_incremental_sql(incremental_strategy, temp_relation, target_relation, unique_key, dest_columns) %}}

  {% endif %}

  {% call statement("main") %}
      {{ build_sql }}
  {% endcall %}

  {% if need_swap %}
      {% do adapter.rename_relation(target_relation, backup_relation) %}
      {% do adapter.rename_relation(intermediate_relation, target_relation) %}
      {% do to_drop.append(backup_relation) %}
  {% endif %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {% if existing_relation is none or existing_relation.is_view or should_full_refresh() %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
