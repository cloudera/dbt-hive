import pytest
import re

from dbt.tests.adapter.basic.test_incremental import (
    BaseIncremental,
)
from dbt.tests.adapter.basic.files import (
    base_table_sql,
    base_materialized_var_sql,
    schema_base_yml,
)

from tests.functional.adapter.test_basic import (
    TestSimpleMaterializationsHive,
)
from tests.functional.adapter.config_files import insertoverwrite_sql


def prepend_attr_in_model_config(model, prop):
    pattern = r"(config\s*\(\s*())"
    result = re.sub(
        pattern,
        lambda x: x.group(1) + prop,
        model,
    )
    print(result)
    return result


class TestSimpleMaterializationsORC(TestSimpleMaterializationsHive):
    @pytest.fixture(scope="class")
    def models(self):
        attr = 'external=True, file_format="orc", '
        return {
            "table_model.sql": prepend_attr_in_model_config(base_table_sql, attr),
            "swappable.sql": prepend_attr_in_model_config(base_materialized_var_sql, attr),
            "schema.yml": schema_base_yml,
        }


class TestSimpleMaterializationsParquet(TestSimpleMaterializationsHive):
    @pytest.fixture(scope="class")
    def models(self):
        attr = 'external=True, file_format="parquet", '
        return {
            "table_model.sql": prepend_attr_in_model_config(base_table_sql, attr),
            "swappable.sql": prepend_attr_in_model_config(base_materialized_var_sql, attr),
            "schema.yml": schema_base_yml,
        }


class TestSimpleMaterializationsAvro(TestSimpleMaterializationsHive):
    @pytest.fixture(scope="class")
    def models(self):
        attr = 'external=True, file_format="avro", '
        return {
            "table_model.sql": prepend_attr_in_model_config(base_table_sql, attr),
            "swappable.sql": prepend_attr_in_model_config(base_materialized_var_sql, attr),
            "schema.yml": schema_base_yml,
        }


class TestInsertOverwriteHiveORC(BaseIncremental):
    @pytest.fixture(scope="class")
    def models(self):
        attr = 'external=True, file_format="orc", '
        return {
            "incremental.sql": prepend_attr_in_model_config(insertoverwrite_sql, attr),
            "schema.yml": schema_base_yml,
        }
