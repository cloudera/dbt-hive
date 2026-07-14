import pytest
from dbt.tests.adapter.constraints.test_constraints import (
    BaseModelConstraintsRuntimeEnforcement,
    BaseTableConstraintsColumnsEqual,
    BaseViewConstraintsColumnsEqual,
    BaseIncrementalConstraintsColumnsEqual,
    BaseConstraintsRuntimeDdlEnforcement,
    BaseConstraintsRollback,
    BaseIncrementalConstraintsRuntimeDdlEnforcement,
    BaseIncrementalConstraintsRollback,
    BaseConstraintQuotedColumn,
)

from dbt.tests.adapter.constraints.fixtures import (
    constrained_model_schema_yml,
    model_schema_yml,
    my_model_sql,
    my_model_wrong_order_sql,
    my_model_wrong_name_sql,
    my_model_view_wrong_order_sql,
    my_model_view_wrong_name_sql,
    my_model_incremental_wrong_order_sql,
    my_model_incremental_wrong_name_sql,
    my_incremental_model_sql,
    model_fk_constraint_schema_yml,
    my_model_wrong_order_depends_on_fk_sql,
    foreign_key_model_sql,
    my_model_incremental_wrong_order_depends_on_fk_sql,
    my_model_with_quoted_column_name_sql,
    model_quoted_column_schema_yml,
)

# Hive-Specific Expected SQL & Model Adjustments
_expected_sql_hive = """
create table <model_identifier>(id integer not null,color string,date_day string)
stored as parquet ;
insert into <model_identifier>(id,color,date_day)
select id,color,date_day from(
-- depends_on: <foreign_key_model_identifier>
select 'blue' as color,1 as id,'2019-01-01' as date_day
)as model_subq
"""

# Hive does not support a 'text' data type so using 'string'
constraints_yml = model_schema_yml.replace("text", "string")
fk_constraints_yml = model_fk_constraint_schema_yml.replace("text", "string")
model_constraints_yml = constrained_model_schema_yml.replace("text", "string")


class HiveSetup:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+file_format": "parquet",
            }
        }

    @pytest.fixture
    def string_type(self):
        return "STRING"

    @pytest.fixture
    def int_type(self):
        return "INT"

    @pytest.fixture
    def schema_string_type(self):
        return "STRING"

    @pytest.fixture
    def schema_int_type(self):
        return "INT"

    @pytest.fixture
    def data_types(self, int_type, schema_int_type, string_type, schema_string_type):
        return [
            ["1", schema_int_type, int_type],
            ["'1'", string_type, string_type],
            ["true", "boolean", "BOOLEAN"],
            ["CAST('2013-11-03 00:00:00' AS timestamp)", "timestamp", "TIMESTAMP"],
            ["CAST(1.23 AS decimal(10,2))", "decimal(10,2)", "DECIMAL"],
            ["ARRAY('a','b','c')", "ARRAY<STRING>", "ARRAY"],
            ["MAP('bar','baz')", "map<string,string>", "MAP"],
        ]


# ------------------------------------------------------------------------------
# Hive Column Equality Tests
# ------------------------------------------------------------------------------
class TestHiveTableConstraintsColumnsEqual(HiveSetup, BaseTableConstraintsColumnsEqual):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_wrong_name_sql,
            "constraints_schema.yml": constraints_yml,
        }


class TestHiveViewConstraintsColumnsEqual(HiveSetup, BaseViewConstraintsColumnsEqual):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_view_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_view_wrong_name_sql,
            "constraints_schema.yml": constraints_yml,
        }


class TestHiveIncrementalConstraintsColumnsEqual(
    HiveSetup, BaseIncrementalConstraintsColumnsEqual
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_incremental_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_incremental_wrong_name_sql,
            "constraints_schema.yml": constraints_yml,
        }


# ------------------------------------------------------------------------------
# Hive DDL Constraint Enforcement Tests
# ------------------------------------------------------------------------------
class BaseHiveConstraintsDdlEnforcementSetup:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+file_format": "parquet"}}

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return _expected_sql_hive


class TestHiveTableConstraintsDdlEnforcement(
    BaseHiveConstraintsDdlEnforcementSetup, BaseConstraintsRuntimeDdlEnforcement
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_wrong_order_depends_on_fk_sql,
            "foreign_key_model.sql": foreign_key_model_sql,
            "constraints_schema.yml": fk_constraints_yml,
        }


class TestHiveIncrementalConstraintsDdlEnforcement(
    BaseHiveConstraintsDdlEnforcementSetup, BaseIncrementalConstraintsRuntimeDdlEnforcement
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_incremental_wrong_order_depends_on_fk_sql,
            "foreign_key_model.sql": foreign_key_model_sql,
            "constraints_schema.yml": fk_constraints_yml,
        }


# ------------------------------------------------------------------------------
# Hive Quoted Column Constraint Test
# ------------------------------------------------------------------------------
class TestHiveConstraintQuotedColumn(HiveSetup, BaseConstraintQuotedColumn):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_with_quoted_column_name_sql,
            "constraints_schema.yml": model_quoted_column_schema_yml.replace(
                "text", "string"
            ).replace('"from"', "`from`"),
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return """
  create table <model_identifier>(id integer not null ,`from` string not null,date_day string)
stored as parquet ;
insert into <model_identifier>(id,`from`,date_day)
select id,`from`,date_day from(
select 'blue' as `from`,1 as id,'2019-01-01' as date_day
)as model_subq
"""


# ------------------------------------------------------------------------------
# Hive Constraint Rollback Tests
# ------------------------------------------------------------------------------
class BaseHiveConstraintsRollbackSetup:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+file_format": "orc", "+contract": {"enforced": True}}}

    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return [
            "Constraint violation",
            "NOT NULL constraint violated",
            "CHECK constraint failed",
            "column is null",
        ]

    def assert_expected_error_messages(self, error_message, expected_error_messages):
        assert any(msg in error_message for msg in expected_error_messages)


class TestHiveTableConstraintsRollback(BaseHiveConstraintsRollbackSetup, BaseConstraintsRollback):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "constraints_schema.yml": constraints_yml,
        }

    @pytest.fixture(scope="class")
    def expected_color(self):
        return "blue"


class TestHiveIncrementalConstraintsRollback(
    BaseHiveConstraintsRollbackSetup, BaseIncrementalConstraintsRollback
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_incremental_model_sql,
            "constraints_schema.yml": constraints_yml,
        }


# ------------------------------------------------------------------------------
# Hive Model-Level Runtime Enforcement
# ------------------------------------------------------------------------------
class TestHiveModelConstraintsRuntimeEnforcement(BaseModelConstraintsRuntimeEnforcement):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+file_format": "parquet"}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_wrong_order_depends_on_fk_sql,
            "foreign_key_model.sql": foreign_key_model_sql,
            "constraints_schema.yml": fk_constraints_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return _expected_sql_hive
