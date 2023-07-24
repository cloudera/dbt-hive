import pytest
import re

from dbt.tests.util import (
    check_relations_equal,
    run_dbt,
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

from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChangeSetup,
)


# class BaseIncrementalOnSchemaChangeSetup:
#     @pytest.fixture(scope="class")
#     def models(self):
#         return {
#             "incremental_sync_remove_only.sql": _MODELS__INCREMENTAL_SYNC_REMOVE_ONLY,
#             "incremental_ignore.sql": _MODELS__INCREMENTAL_IGNORE,
#             "incremental_sync_remove_only_target.sql":
#                 _MODELS__INCREMENTAL_SYNC_REMOVE_ONLY_TARGET,
#             "incremental_ignore_target.sql": _MODELS__INCREMENTAL_IGNORE_TARGET,
#             "incremental_fail.sql": _MODELS__INCREMENTAL_FAIL,
#             "incremental_sync_all_columns.sql": _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS,
#             "incremental_append_new_columns_remove_one.sql":
#                 _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE,
#             "model_a.sql": _MODELS__A,
#             "incremental_append_new_columns_target.sql":
#                 _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_TARGET,
#             "incremental_append_new_columns.sql": _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS,
#             "incremental_sync_all_columns_target.sql": _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_TARGET,
#             "incremental_append_new_columns_remove_one_target.sql": _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE_TARGET,
#         }

#     def run_twice_and_assert(self, include, compare_source, compare_target, project):

#         # dbt run (twice)
#         run_args = ['run']
#         if include:
#             run_args.extend(('--select', include))
#         results_one = run_dbt(run_args)
#         assert len(results_one) == 3

#         results_two = run_dbt(run_args)
#         assert len(results_two) == 3

#         check_relations_equal(project.adapter, [compare_source, compare_target])

#     def run_incremental_sync_all_columns(self, project):
#         select = 'model_a incremental_sync_all_columns incremental_sync_all_columns_target'
#         compare_source = 'incremental_sync_all_columns'
#         compare_target = 'incremental_sync_all_columns_target'
#         self.run_twice_and_assert(select, compare_source, compare_target, project)


# class BaseIncrementalOnSchemaChange(BaseIncrementalOnSchemaChangeSetup):
#     def test_run_incremental_sync_all_columns(self, project):
#         self.run_incremental_sync_all_columns(project)
#         self.run_incremental_sync_remove_only(project)


# class TestIncrementalOnSchemaChange(BaseIncrementalOnSchemaChange):
#     pass


class BaseIcebergIncrementalOnSchemaChangeSetup(BaseIncrementalOnSchemaChangeSetup):
    def replace_with_iceberg(self, model):
        pattern = r"(config\s*\([^)]*\))"
        result = re.sub(
            pattern, lambda x: x.group(0).rstrip(")") + ", table_type='iceberg')", model
        )
        print(result)
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
            "incremental_fail.sql": _MODELS__INCREMENTAL_FAIL,
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


class BaseIcebergIncrementalOnSchemaChange(BaseIcebergIncrementalOnSchemaChangeSetup):
    def test_run_incremental_sync_all_columns(self, project):
        self.run_incremental_sync_all_columns(project)
        self.run_incremental_sync_remove_only(project)


class TestIcebergIncrementalOnSchemaChange(BaseIcebergIncrementalOnSchemaChange):
    pass
