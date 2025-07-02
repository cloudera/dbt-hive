import pytest

from dbt.tests.util import run_dbt_and_capture

from dbt.tests.adapter.incremental.test_incremental_microbatch import (
    BaseMicrobatch,
    patch_microbatch_end_time,
)

_input_timestamp_modified_model_sql = """
{{ config(materialized='table', event_time='event_time') }}
select 1 as id, TIMESTAMP '2020-01-01 00:00:00' as event_time
union all
select 2 as id, TIMESTAMP '2020-01-02 00:00:00' as event_time
union all
select 3 as id, TIMESTAMP '2020-01-03 00:00:00' as event_time
"""

_microbatch_uniqueid_modified_model_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_time',
    batch_size='day',
    partition_by='date_day',
    begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0))
}}
select *, to_date(event_time) as date_day from {{ ref('input_model') }}
"""
_microbatch_no_partition_by_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_time',
    batch_size='day',
    begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0))
}}
select *, to_date(event_time) as date_day from {{ ref('input_model') }}
"""


class TestHiveMicrobatch(BaseMicrobatch):
    @pytest.fixture(scope="class")
    def input_model_sql(self) -> str:
        return _input_timestamp_modified_model_sql

    @pytest.fixture(scope="class")
    def microbatch_model_sql(self) -> str:
        return _microbatch_uniqueid_modified_model_sql

    @pytest.fixture(scope="class")
    def insert_two_rows_sql(self, project) -> str:
        test_schema_relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema
        )
        return f"insert into {test_schema_relation}.input_model (id, event_time) values (4, TIMESTAMP '2020-01-04 00:00:00'), (5, TIMESTAMP '2020-01-05 00:00:00')"


class TestHiveMicroBatchNoPartitionKey:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "microbatch.sql": _microbatch_no_partition_by_sql,
            "input_model.sql": _input_timestamp_modified_model_sql,
        }

    def test_no_partition_by(self, project):
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            _, stdout = run_dbt_and_capture(["run"], expect_pass=False)
        assert (
            "dbt-hive 'microbatch' incremental strategy requires a `partition_by` config" in stdout
        )
