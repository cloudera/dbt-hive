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

{% materialization table, adapter = 'hive' %}

  {%- set identifier = model['alias'] -%}
  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier, schema=schema, database=database, type='table') -%}

  {{ run_hooks(pre_hooks) }}

 {%- set table_type = config.get('table_type', '') | lower -%}
  {% if table_type == 'iceberg' %}
      {% if old_relation %}
          {{ adapter.drop_relation(old_relation) }}
      {% endif %}
      {% call statement('main') -%}
          {{ create_table_as(False, target_relation, sql) }}
      {%- endcall %}

  {% else %}

      {%- set tmp_relation = target_relation.incorporate(path={"identifier": identifier ~ "__dbt_tmp"}) -%}

      {% call statement('main') -%}
          {{ create_table_as(False, tmp_relation, sql) }}
      {%- endcall %}

      {% if old_relation %}
          {{ adapter.drop_relation(old_relation) }}
      {% endif %}

      {{ adapter.rename_relation(tmp_relation, target_relation) }}
  {% endif %}

  {% set grant_config = config.get('grants') %}
  {% set should_revoke = should_revoke(target_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}
  {% do persist_docs(target_relation, model) %}
  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]})}}

{% endmaterialization %}
