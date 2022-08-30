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

{% macro get_insert_overwrite_sql(source_relation, target_relation) %}
    
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    insert overwrite table {{ target_relation }}
    {{ partition_cols(label="partition") }}
    select {{dest_cols_csv}} from {{ source_relation.include(database=false, schema=true) }}

{% endmacro %}


{% macro get_insert_into_sql(source_relation, target_relation) %}

    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    insert into table {{ target_relation }}
    select {{dest_cols_csv}} from {{ source_relation.include(database=false, schema=true) }}

{% endmacro %}


{% macro get_update_csv(column_names, qualifier='') %}

    {% set quoted = [] %}
    {% for col in column_names -%}
        {% if qualifier != '' %}
          {%- do quoted.append(qualifier+'.'+col) -%}
        {% else %}
          {%- do quoted.append(col) -%}
        {% endif %}
    {%- endfor %}

    {%- set dest_cols_csv = quoted | join(', ') -%}
    {{ return(dest_cols_csv) }}

{% endmacro %}

{% macro hive__get_merge_sql(target, source, unique_key, dest_columns, predicates=none) %}
  {%- set update_columns = config.get("merge_update_columns") -%}
  {%- set update_cols_csv = get_update_csv(update_columns, 'DBT_INTERNAL_SOURCE') -%}
  
  {% set merge_condition %}
    {% if unique_key %}
        on DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
    {% else %}
        on false
    {% endif %}
  {% endset %}
  
    merge into {{ target }} as DBT_INTERNAL_DEST
      using {{ source.include(schema=true) }} as DBT_INTERNAL_SOURCE
      
      {{ merge_condition }}
      
      when matched then update set
        {% if update_columns -%}{%- for column_name in update_columns %}
            {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
        {%- else %} * {% endif %}
    
      when not matched then insert 
        ({{get_update_csv(update_columns)}})
      values 
        ({{update_cols_csv}})
{% endmacro %}


{% macro dbt_hive_get_incremental_sql(strategy, source, target, unique_key) %}
  {%- if strategy == 'append' -%}
    {#-- insert new records into existing table, without updating or overwriting #}
    {{ get_insert_into_sql(source, target) }}
  {%- elif strategy == 'insert_overwrite' -%}
    {#-- insert statements don't like CTEs, so support them via a temp view #}
    {{ get_insert_overwrite_sql(source, target) }}
  {%- elif strategy == 'merge' -%}
  {#-- merge all columns with databricks delta - schema changes are handled for us #}
    {{ get_merge_sql(target, source, unique_key, dest_columns=none, predicates=none) }}
  {%- else -%}
    {% set no_sql_for_strategy_msg -%}
      No known SQL for the incremental strategy provided: {{ strategy }}
    {%- endset %}
    {%- do exceptions.raise_compiler_error(no_sql_for_strategy_msg) -%}
  {%- endif -%}

{% endmacro %}
