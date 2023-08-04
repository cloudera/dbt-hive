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

import pytest

from dbt.tests.util import (
    run_dbt,
    check_result_nodes_by_name,
    relation_from_name,
    check_relation_types,
    check_relations_equal,
)

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_incremental import (
    BaseIncremental,
)

from dbt.tests.adapter.basic.files import (
    model_base,
    model_incremental,
    base_view_sql,
    schema_base_yml,
)


def is_iceberg_table(project, tableName):
    rows = project.run_sql(f"describe formatted {tableName}", fetch="all")
    result = False
    for col_name, data_type, comment in rows:
        if (
            data_type
            and data_type.strip() == "table_type"
            and comment
            and comment.strip() == "ICEBERG"
        ):
            result = True
    return result


iceberg_base_table_sql = (
    """
{{
  config(
    materialized="table",
    table_type="iceberg"
  )
}}""".strip()
    + model_base
)

iceberg_base_materialized_var_sql = (
    """
{{
  config(
    materialized=var("materialized_var", "table"),
    table_type="iceberg"
  )
}}""".strip()
    + model_base
)

incremental_iceberg_sql = (
    """
 {{
    config(
        materialized="incremental",
        table_type="iceberg"
    )
}}
""".strip()
    + model_incremental
)


incremental_partition_iceberg_sql = """
 {{
    config(
        materialized="incremental",
        partition_by="id",
        table_type="iceberg"
    )
}}
select *, id as id_partition1 from {{ source('raw', 'seed') }}
{% if is_incremental() %}
    where id > (select max(id) from {{ this }})
{% endif %}
""".strip()

incremental_multiple_partition_iceberg_sql = """
 {{
    config(
        materialized="incremental",
        partition_by=["id_partition1", "id_partition2"],
        table_type="iceberg"
    )
}}
select *, id as id_partition1, id as id_partition2 from {{ source('raw', 'seed') }}
{% if is_incremental() %}
    where id > (select max(id) from {{ this }})
{% endif %}
""".strip()

insertoverwrite_iceberg_sql = """
 {{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by="id_partition1",
        table_type="iceberg"
    )
}}
select *, id as id_partition1 from {{ source('raw', 'seed') }}
{% if is_incremental() %}
    where id > (select max(id) from {{ this }})
{% endif %}
""".strip()


# For iceberg table formats, check_relations_equal util is not working as expected
# Impala upstream issue: https://issues.apache.org/jira/browse/IMPALA-12097
# Hence removing this check from unit tests
class TestSimpleMaterializationsIcebergHive(BaseSimpleMaterializations):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_model.sql": base_view_sql,
            "table_model.sql": iceberg_base_table_sql,
            "swappable.sql": iceberg_base_materialized_var_sql,
            "schema.yml": schema_base_yml,
        }

    def test_base(self, project):
        # seed command
        results = run_dbt(["seed"])
        # seed result length
        assert len(results) == 1

        # run command
        results = run_dbt()
        # run result length
        assert len(results) == 3

        # names exist in result nodes
        check_result_nodes_by_name(results, ["view_model", "table_model", "swappable"])

        # check relation types
        expected = {
            "base": "table",
            "view_model": "view",
            "table_model": "table",
            "swappable": "table",
        }
        check_relation_types(project.adapter, expected)

        assert (
            is_iceberg_table(project, relation_from_name(project.adapter, "table_model")) == True
        )
        assert is_iceberg_table(project, relation_from_name(project.adapter, "swappable")) == True

        # base table rowcount
        relation = relation_from_name(project.adapter, "base")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10

        # Uncomment the below check after IMPALA-12097 gets resolved
        # check_relations_equal(project.adapter, ["base", "view_model", "table_model", "swappable"])

        # check relations in catalog
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 4
        assert len(catalog.sources) == 1

        # run_dbt changing materialized_var to incremental
        results = run_dbt(
            ["run", "--full-refresh", "-m", "swappable", "--vars", "materialized_var: incremental"]
        )
        assert len(results) == 1
        assert is_iceberg_table(project, relation_from_name(project.adapter, "swappable")) == True

        # check relation types, swappable is table
        expected = {
            "base": "table",
            "view_model": "view",
            "table_model": "table",
            "swappable": "table",
        }
        check_relation_types(project.adapter, expected)

        # run_dbt changing materialized_var to view
        results = run_dbt(["run", "-m", "swappable", "--vars", "materialized_var: view"])
        assert len(results) == 1
        assert is_iceberg_table(project, relation_from_name(project.adapter, "swappable")) == False

        # check relation types, swappable is view
        expected = {
            "base": "table",
            "view_model": "view",
            "table_model": "table",
            "swappable": "view",
        }
        check_relation_types(project.adapter, expected)


class TestIncrementalIcebergHive(BaseIncremental):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_test_model.sql": incremental_iceberg_sql,
            "schema.yml": schema_base_yml,
        }

    def test_incremental(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 2

        # base table rowcount
        relation = relation_from_name(project.adapter, "base")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10

        # added table rowcount
        relation = relation_from_name(project.adapter, "added")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 20

        # run command
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["run", "--vars", "seed_name: base"])
        assert len(results) == 1
        assert (
            is_iceberg_table(
                project, relation_from_name(project.adapter, "incremental_test_model")
            )
            == True
        )

        # check relations equal
        check_relations_equal(project.adapter, ["base", "incremental_test_model"])

        # change seed_name var
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["run", "--vars", "seed_name: added"])
        assert len(results) == 1
        assert (
            is_iceberg_table(
                project, relation_from_name(project.adapter, "incremental_test_model")
            )
            == True
        )

        # check relations equal
        check_relations_equal(project.adapter, ["added", "incremental_test_model"])

        # get catalog from docs generate
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 3
        assert len(catalog.sources) == 1


class TestIncrementalPartitionIcebergHive(TestIncrementalIcebergHive):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_test_model.sql": incremental_partition_iceberg_sql,
            "schema.yml": schema_base_yml,
        }


class TestIncrementalMultiplePartitionIcebergHive(TestIncrementalIcebergHive):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_test_model.sql": incremental_multiple_partition_iceberg_sql,
            "schema.yml": schema_base_yml,
        }


class TestInsertOverwriteIcebergHive(TestIncrementalIcebergHive):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_test_model.sql": insertoverwrite_iceberg_sql,
            "schema.yml": schema_base_yml,
        }
