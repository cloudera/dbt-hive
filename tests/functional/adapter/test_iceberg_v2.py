import pytest
import re

from tests.functional.adapter.iceberg_files import (
    iceberg_base_table_sql,
    incremental_iceberg_sql,
    incremental_partition_iceberg_sql,
    incremental_multiple_partition_iceberg_sql,
    insertoverwrite_iceberg_sql,
)

from tests.functional.adapter.test_iceberg_v1 import (
    BaseSimpleMaterializationsForIceberg,
    BaseIncrementalForIceberg,
)

from dbt.tests.adapter.basic.files import model_base, base_view_sql, schema_base_yml

iceberg_base_materialized_var_sql = (
    """
    {{
      config(
        materialized=var("materialized_var", "table"),
        table_type="iceberg",
        tbl_properties={"format-version": "2"}
      )
    }}""".strip()
    + model_base
)


def replace_with_iceberg_v2(model):
    # Add iceberg table_type as a config parameter for existing tests
    pattern = r"(config\s*\([^)]*\))"
    result = re.sub(
        pattern,
        lambda x: x.group(0).rstrip(")") + ', tbl_properties={"format-version": "2"})',
        model,
    )
    return result


class TestSimpleMaterializationsIcebergV2Hive(BaseSimpleMaterializationsForIceberg):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_model.sql": base_view_sql,
            "table_model.sql": replace_with_iceberg_v2(iceberg_base_table_sql),
            "swappable.sql": iceberg_base_materialized_var_sql,
            "schema.yml": schema_base_yml,
        }


class TestIncrementalIcebergV2Hive(BaseIncrementalForIceberg):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_test_model.sql": replace_with_iceberg_v2(incremental_iceberg_sql),
            "schema.yml": schema_base_yml,
        }


class TestIncrementalPartitionIcebergV2Hive(BaseIncrementalForIceberg):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_test_model.sql": replace_with_iceberg_v2(
                incremental_partition_iceberg_sql
            ),
            "schema.yml": schema_base_yml,
        }


class TestIncrementalMultiplePartitionIcebergV2Hive(BaseIncrementalForIceberg):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_test_model.sql": replace_with_iceberg_v2(
                incremental_multiple_partition_iceberg_sql
            ),
            "schema.yml": schema_base_yml,
        }


class TestInsertOverwriteIcebergV2Hive(BaseIncrementalForIceberg):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_test_model.sql": replace_with_iceberg_v2(insertoverwrite_iceberg_sql),
            "schema.yml": schema_base_yml,
        }
