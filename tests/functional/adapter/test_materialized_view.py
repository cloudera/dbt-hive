import pytest
import re

from typing import Optional, Tuple
from dbt.tests.adapter.materialized_view.basic import MaterializedViewBasic
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.hive.relation import HiveRelationType


class TestHiveMaterializedviewBasic(MaterializedViewBasic):
    @staticmethod
    def insert_record(project, table: BaseRelation, record: Tuple[int, int]):
        my_id, value = record
        project.run_sql(f"insert into {table} (id, value) values ({my_id}, {value})")

    @staticmethod
    def refresh_materialized_view(project, materialized_view: BaseRelation):
        sql = f"alter materialized view {materialized_view} rebuild"
        project.run_sql(sql)

    @staticmethod
    def query_row_count(project, relation: BaseRelation) -> int:
        sql = f"select count(*) from {relation}"
        return project.run_sql(sql, fetch="one")[0]

    @staticmethod
    def query_relation_type(project, relation: BaseRelation) -> Optional[str]:
        sql = f"describe formatted {relation}"
        describe_output = project.run_sql(sql, fetch="all")
        describe_output_str = str(describe_output)
        describe_without_spaces = describe_output_str.replace(" ", "")
        pattern = re.compile(r"(?<=TableType:',')(\w+)")
        relation_type = pattern.search(describe_without_spaces).group(0)
        if relation_type == "VIRTUAL_VIEW":
            return HiveRelationType.View
        elif relation_type == "MANAGED_TABLE" or relation_type == "EXTERNAL_TABLE":
            return HiveRelationType.Table
        else:
            return HiveRelationType.MaterializedView
