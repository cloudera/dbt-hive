import json

import pytest

from dbt.tests.util import run_dbt

_MODEL__PARTITIONED_TABLE = """
{{ config(materialized='table', partition_by=['year_raff', 'month_raff']) }}
select
    1 as id,
    'a' as name,
    2024 as year_raff,
    1 as month_raff
"""

_SCHEMA_YML__PARTITIONED_TABLE = """
version: 2
models:
  - name: partitioned_table
    description: "A partitioned table description"
    columns:
      - name: id
        description: "id column description"
      - name: name
        description: "name column description"
      - name: year_raff
        description: "year partition column description"
      - name: month_raff
        description: "month partition column description"
"""


class TestPersistDocsPartitioned:
    """
    Regression test: persist_docs used to fail for partitioned tables because
    Hive requires `ALTER TABLE ... PARTITION COLUMN (...)` (rather than
    `CHANGE COLUMN`) to set a comment on a partitioning column, and because
    the generated `CREATE TABLE` DDL previously placed `COMMENT` after
    `PARTITIONED BY`, which Hive's parser rejects.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"partitioned_table.sql": _MODEL__PARTITIONED_TABLE}

    @pytest.fixture(scope="class")
    def properties(self):
        return {"schema.yml": _SCHEMA_YML__PARTITIONED_TABLE}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+persist_docs": {
                        "relation": True,
                        "columns": True,
                    },
                }
            }
        }

    def test_persist_docs_on_partitioned_table(self, project):
        # The model must build successfully: this fails outright if the
        # COMMENT clause is emitted in the wrong position relative to
        # PARTITIONED BY.
        run_dbt()

        # Column comments must apply cleanly to both regular and partition
        # columns: this fails if a partition column is altered with
        # CHANGE COLUMN instead of PARTITION COLUMN.
        run_dbt(["docs", "generate"])
        with open("target/catalog.json") as fp:
            catalog_data = json.load(fp)

        table_node = catalog_data["nodes"]["model.test.partitioned_table"]

        assert table_node["metadata"]["comment"].startswith("A partitioned table description")
        assert table_node["columns"]["id"]["comment"].startswith("id column description")
        assert table_node["columns"]["name"]["comment"].startswith("name column description")
        assert table_node["columns"]["year_raff"]["comment"].startswith(
            "year partition column description"
        )
        assert table_node["columns"]["month_raff"]["comment"].startswith(
            "month partition column description"
        )
