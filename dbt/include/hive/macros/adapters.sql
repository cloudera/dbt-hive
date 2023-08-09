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

{% macro file_format_clause() %}
  {%- set file_format = config.get('file_format', validator=validation.any[basestring]) -%}
  {%- if file_format is not none %}
    stored as {{ file_format }}
  {%- endif %}
{%- endmacro -%}

{% macro location_clause() %}
  {%- set location_root = config.get('location_root', validator=validation.any[basestring]) -%}
  {%- set identifier = model['alias'] -%}
  {%- if location_root is not none %}
    location '{{ location_root }}/{{ identifier }}'
  {%- endif %}
{%- endmacro -%}

{% macro options_clause() -%}
  {%- set options = config.get('options') -%}
  {%- if options is not none %}
    options (
      {%- for option in options -%}
      {{ option }} "{{ options[option] }}" {% if not loop.last %}, {% endif %}
      {%- endfor %}
    )
  {%- endif %}
{%- endmacro -%}

{% macro comment_clause() %}
  {%- set raw_persist_docs = config.get('persist_docs', {}) -%}

  {%- if raw_persist_docs is mapping -%}
    {%- set raw_relation = raw_persist_docs.get('relation', false) -%}
      {%- if raw_relation -%}
      comment '{{ model.description | replace("'", "\\'") }}'
      {% endif %}
  {%- else -%}
    {{ exceptions.raise_compiler_error("Invalid value provided for 'persist_docs'. Expected dict but got value: " ~ raw_persist_docs) }}
  {% endif %}
{%- endmacro -%}

{% macro partition_cols(label, required=false) %}
  {%- set cols = config.get('partition_by', validator=validation.any[list, basestring]) -%}
  {%- if cols is not none %}
    {%- if cols is string -%}
      {%- set cols = [cols] -%}
    {%- endif -%}
    {{ label }} (
    {%- for item in cols -%}
      {{ item }}
      {%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
    )
  {%- endif %}
{%- endmacro -%}


{% macro clustered_cols(label, required=false) %}
  {%- set cols = config.get('clustered_by', validator=validation.any[list, basestring]) -%}
  {%- set buckets = config.get('buckets', validator=validation.any[int]) -%}
  {%- if (cols is not none) and (buckets is not none) %}
    {%- if cols is string -%}
      {%- set cols = [cols] -%}
    {%- endif -%}
    {{ label }} (
    {%- for item in cols -%}
      {{ item }}
      {%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
    ) into {{ buckets }} buckets
  {%- endif %}
{%- endmacro -%}

{% macro fetch_tbl_properties(relation) -%}
  {% call statement('list_properties', fetch_result=True) -%}
    SHOW TBLPROPERTIES {{ relation }}
  {% endcall %}
  {% do return(load_result('list_properties').table) %}
{%- endmacro %}


{% macro create_temporary_view(relation, sql) -%}
  --  We can't use temporary tables with `create ... as ()` syntax in Hive2
  -- create temporary view {{ relation.include(schema=false) }} as
  create temporary table {{ relation.include(schema=false) }} as
    {{ sql }}
{% endmacro %}


{% macro properties_clause(properties) %}
  {%- if properties is not none -%}
      TBLPROPERTIES (
          {%- for key, value in properties.items() -%}
            "{{ key }}" = "{{ value }}"
            {%- if not loop.last -%}{{ ',\n  ' }}{%- endif -%}
          {%- endfor -%}
      )
  {%- endif -%}
{%- endmacro -%}

{% macro stored_by_clause(table_type) %}
  {%- if table_type is not none %}
    stored by {{ table_type }}
  {%- endif %}
{%- endmacro -%}

{% macro hive__create_table_as(temporary, relation, sql) -%}
  {%- set _properties = config.get('tbl_properties') -%}
  {%- set is_external = config.get('external') -%}
  {%- set table_type = config.get('table_type') -%}

  {% if temporary -%}
    {{ create_temporary_view(relation, sql) }}
  {%- else -%}
    {% if config.get('file_format', validator=validation.any[basestring]) == 'delta' %}
      create or replace table {{ relation }}
    {% else %}
      create {% if is_external == true -%}external{%- endif %} table {{ relation }}
    {% endif %}
    {{ options_clause() }}
    {% if table_type == 'iceberg' -%}
      {{ partition_cols(label="partitioned by spec") }}
    {% else %}
      {{ partition_cols(label="partitioned by") }}
    {%- endif %}
    {{ clustered_cols(label="clustered by") }}
    {{ stored_by_clause(table_type) }}
    {{ file_format_clause() }}
    {{ location_clause() }}
    {{ comment_clause() }}
    {{ properties_clause(_properties) }}
    as
      {{ sql }}
  {%- endif %}
{%- endmacro -%}


{% macro hive__create_view_as(relation, sql) -%}
  create or replace view {{ relation }}
  {{ comment_clause() }}
  as
    {{ sql }}
{% endmacro %}

{% macro hive__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    create schema if not exists {{relation}}
  {% endcall %}
{% endmacro %}

{% macro hive__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}
    drop schema if exists {{ relation }} cascade
  {%- endcall -%}
{% endmacro %}

{# use describe extended for more information #}
{% macro hive__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
    describe formatted {{ relation }}
  {% endcall %}
  {% do return(load_result('get_columns_in_relation').table) %}
{% endmacro %}

{% macro hive__list_relations_without_caching(relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    show table extended in {{ relation }} like '*'
  {% endcall %}

  {% do return(load_result('list_relations_without_caching').table) %}
{% endmacro %}

{% macro hive__list_schemas(database) -%}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
    show databases
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro hive__rename_relation(from_relation, to_relation) -%}
  {% call statement('rename_relation') -%}
    {% if not from_relation.type %}
      {% do exceptions.raise_database_error("Cannot rename a relation with a blank type: " ~ from_relation.identifier) %}
    {% elif from_relation.type in ('table') %}
        alter table {{ from_relation }} rename to {{ to_relation }}
    {% elif from_relation.type == 'view' %}
        alter view {{ from_relation }} rename to {{ to_relation }}
    {% else %}
      {% do exceptions.raise_database_error("Unknown type '" ~ from_relation.type ~ "' for relation: " ~ from_relation.identifier) %}
    {% endif %}
  {%- endcall %}
{% endmacro %}

{% macro hive__drop_relation(relation) -%}
  {% call statement('drop_relation_if_exists_table') %}
    drop table if exists {{ relation }}
  {% endcall %}
  {% call statement('drop_relation_if_exists_view') %}
    drop view if exists {{ relation }}
  {% endcall %}
{% endmacro %}


{% macro hive__generate_database_name(custom_database_name=none, node=none) -%}
  {% do return(None) %}
{%- endmacro %}

{% macro hive__persist_docs(relation, model, for_relation, for_columns) -%}
  {% if for_columns and config.persist_column_docs() and model.columns %}
    {% do alter_column_comment(relation, model.columns) %}
  {% endif %}
{% endmacro %}

{% macro hive__alter_column_comment(relation, column_dict) %}
  {% if config.get('file_format', validator=validation.any[basestring]) == 'delta' %}
    {% for column_name in column_dict %}
      {% set comment = column_dict[column_name]['description'] %}
      {% set escaped_comment = comment | replace('\'', '\\\'') %}
      {% set comment_query %}
        alter table {{ relation }} change column
            {{ adapter.quote(column_name) if column_dict[column_name]['quote'] else column_name }}
            comment '{{ escaped_comment }}';
      {% endset %}
      {% do run_query(comment_query) %}
    {% endfor %}
  {% endif %}
{% endmacro %}

{% macro hive__list_tables_without_caching(schema) %}
  {% call statement('list_tables_without_caching', fetch_result=True) -%}
    show tables in {{ schema }}
  {% endcall %}
  {% do return(load_result('list_tables_without_caching').table) %}
{% endmacro %}

{% macro hive__list_views_without_caching(schema) %}
  {% call statement('list_views_without_caching', fetch_result=True) -%}
    show views  in {{ schema }}
    -- show tables  in {{ relation }}
    -- hive2 has no `show view` command
  {% endcall %}
  {% do return(load_result('list_views_without_caching').table) %}
{% endmacro %}

{% macro get_hive_version() %}
  {% set version_results = run_query('select version()') %}
  {% if execute %}
     {% set num_rows = (version_results | length) %}

     {% if num_rows == 1 %}
        {% set version_text = version_results[0][0] %}
        {{ log("get_hive_version " ~ version_text) }}
        {% do return(version_text.split(".")[0]) %}
     {% else %}
        {% do return('2') %}  {# assume hive 2 by default #}
     {% endif %}
  {% else %}
    {% do return('2') %}  {# assume hive 2 by default #}
  {% endif %}
{% endmacro %}

{% macro alter_relation_add_columns(relation, add_columns = none) -%}
  {%- set quote_seed_column = model['config'].get('quote_columns', None) -%}

  {% if add_columns is none %}
    {% set add_columns = [] %}
  {% endif %}

  {% set sql -%}
    alter {{ relation.type }} {{ relation }}
      add columns (
        {%- for column in add_columns -%}
          {{ adapter.quote_seed_column(column.name, quote_seed_column) }} {{ column.data_type  }} {{ ',' if not loop.last }}
        {%- endfor -%}
      );
  {%- endset -%}

  {% if (add_columns | length) > 0 %}
    {{ return(run_query(sql)) }}
  {% endif %}
{% endmacro %}

{% macro alter_relation_change_columns(relation, change_columns = none) -%}
  {%- set quote_seed_column = model['config'].get('quote_columns', None) -%}
  {% if change_columns is none %}
    {% set change_columns = [] %}
  {% endif %}

  {% set sql -%}
    {%- for column in change_columns -%}
      alter {{ relation.type }} {{ relation }}
        change {{ adapter.quote_seed_column(column.name, quote_seed_column) }}
          {{ adapter.quote_seed_column(column.name, quote_seed_column) }}
          {{ column.data_type }}
    {%- endfor -%}
  {%- endset -%}

  {% if (change_columns | length) > 0 %}
    {{ return(run_query(sql)) }}
  {% endif %}
{% endmacro %}

{% macro alter_relation_replace_columns(relation, replace_columns = none) -%}
  {%- set quote_seed_column = model['config'].get('quote_columns', None) -%}
  {% if replace_columns is none %}
    {% set replace_columns = [] %}
  {% endif %}

  {% set sql -%}
      alter {{ relation.type }} {{ relation }}
          replace columns (
            {%- for column in replace_columns -%}
              {{ adapter.quote_seed_column(column.name, quote_seed_column) }}
              {{ column.data_type }}
              {{ ', ' if not loop.last }}
            {%- endfor -%}
          )
  {%- endset -%}

  {% if (replace_columns | length) > 0 %}
    {{ return(run_query(sql)) }}
  {% endif %}
{% endmacro %}
