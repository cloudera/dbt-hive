import pytest
import re

from dbt.tests.util import run_dbt

from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChange,
    BaseIncrementalOnSchemaChangeSetup,
)


from dbt.tests.adapter.incremental.fixtures import (
    _MODELS__INCREMENTAL_SYNC_REMOVE_ONLY,
    _MODELS__INCREMENTAL_IGNORE,
    _MODELS__INCREMENTAL_SYNC_REMOVE_ONLY_TARGET,
    _MODELS__INCREMENTAL_IGNORE_TARGET,
    _MODELS__INCREMENTAL_FAIL,
    _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS,
    _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE,
    _MODELS__A,
    _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_TARGET,
    _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS,
    _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_TARGET,
    _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE_TARGET,
)


class BaseIncrementalOnSchemaChange(BaseIncrementalOnSchemaChangeSetup):
    def test_run_incremental_ignore(self, project):
        select = "model_a incremental_ignore incremental_ignore_target"
        compare_source = "incremental_ignore"
        compare_target = "incremental_ignore_target"
        self.run_twice_and_assert(select, compare_source, compare_target, project)

    def test_run_incremental_append_new_columns(self, project):
        self.run_incremental_append_new_columns(project)
        self.run_incremental_append_new_columns_remove_one(project)

    def test_run_incremental_fail_on_schema_change(self, project):
        select = "model_a incremental_fail"
        run_dbt(["run", "--models", select, "--full-refresh"])
        results_two = run_dbt(["run", "--models", select], expect_pass=False)
        assert "Compilation Error" in results_two[1].message


class TestIncrementalOnSchemaChange(BaseIncrementalOnSchemaChange):
    pass


# Schema change tests for incremental merge strategy
class BaseMergeOnSchemaChangeSetup(BaseIncrementalOnSchemaChangeSetup):
    def replace_with_iceberg(self, model):
        # Add iceberg table_type as a config parameter for existing tests
        pattern = r"(config\s*\([^)]*\))"
        result = re.sub(
            pattern, lambda x: x.group(0).rstrip(")") + ", incremental_strategy='merge')", model
        )
        return result

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_sync_remove_only.sql": self.replace_with_iceberg(
                _MODELS__INCREMENTAL_SYNC_REMOVE_ONLY
            ),
            "incremental_ignore.sql": self.replace_with_iceberg(_MODELS__INCREMENTAL_IGNORE),
            "incremental_sync_remove_only_target.sql": self.replace_with_iceberg(
                _MODELS__INCREMENTAL_SYNC_REMOVE_ONLY_TARGET
            ),
            "incremental_ignore_target.sql": self.replace_with_iceberg(
                _MODELS__INCREMENTAL_IGNORE_TARGET
            ),
            "incremental_fail.sql": self.replace_with_iceberg(_MODELS__INCREMENTAL_FAIL),
            "incremental_sync_all_columns.sql": self.replace_with_iceberg(
                _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS
            ),
            "incremental_append_new_columns_remove_one.sql": self.replace_with_iceberg(
                _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE
            ),
            "model_a.sql": _MODELS__A,
            "incremental_append_new_columns_target.sql": self.replace_with_iceberg(
                _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_TARGET
            ),
            "incremental_append_new_columns.sql": self.replace_with_iceberg(
                _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS
            ),
            "incremental_sync_all_columns_target.sql": self.replace_with_iceberg(
                _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_TARGET
            ),
            "incremental_append_new_columns_remove_one_target.sql": self.replace_with_iceberg(
                _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE_TARGET
            ),
        }


class BaseMergeOnSchemaChange(BaseMergeOnSchemaChangeSetup):
    def test_run_incremental_ignore(self, project):
        select = "model_a incremental_ignore incremental_ignore_target"
        compare_source = "incremental_ignore"
        compare_target = "incremental_ignore_target"
        self.run_twice_and_assert(select, compare_source, compare_target, project)

    def test_run_incremental_append_new_columns(self, project):
        self.run_incremental_append_new_columns(project)
        self.run_incremental_append_new_columns_remove_one(project)

    def test_run_incremental_fail_on_schema_change(self, project):
        select = "model_a incremental_fail"
        run_dbt(["run", "--models", select, "--full-refresh"])
        results_two = run_dbt(["run", "--models", select], expect_pass=False)
        assert "Compilation Error" in results_two[1].message


class TestMergeOnSchemaChange(BaseMergeOnSchemaChange):
    pass
