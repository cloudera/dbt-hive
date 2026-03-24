{#
# Copyright 2026 Cloudera Inc.
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

{% macro hive__get_create_materialized_view_as_sql(relation, sql) -%}
   create materialized view if not exists {{ relation }}
     as
    {{ sql }}
{% endmacro %}

{% macro hive__get_drop_sql(relation) -%}
    drop {{ relation.type }} if exists {{ relation.render() }}
{% endmacro %}

{% macro hive__get_refresh_materialized_view_sql(relation) -%}
  alter materialized view {{ relation }} rebuild
{% endmacro %}
