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

from typing import Any
import pytest

from datetime import datetime
from dbt.tests.util import AnyInteger
from dbt.tests.adapter.basic.test_docs_generate import BaseDocsGenerate, BaseDocsGenReferences, verify_catalog

from dbt.tests.util import run_dbt, rm_file

def no_stats():
    return {
        "has_stats": {
            "id": "has_stats",
            "label": "Has Stats?",
            "value": False,
            "description": "Indicates whether there are statistics for this table",
            "include": False,
        },
    }

def base_expected_catalog(
    project,
    role,
    id_type,
    text_type,
    time_type,
    view_type,
    table_type,
    model_stats,
    seed_stats=None,
    case=None,
    case_columns=False,
):

    if case is None:

        def case(x):
            return x

    col_case = case if case_columns else lambda x: x

    if seed_stats is None:
        seed_stats = model_stats

    model_database = project.database
    my_schema_name = case(project.test_schema)
    alternate_schema = case(project.test_schema + "_test")

    expected_cols = {
        col_case("id"): {
            "name": col_case("id"),
            "index": AnyInteger(),
            "type": id_type,
            "comment": None,
        },
        col_case("first_name"): {
            "name": col_case("first_name"),
            "index": AnyInteger(),
            "type": text_type,
            "comment": None,
        },
        col_case("email"): {
            "name": col_case("email"),
            "index": AnyInteger(),
            "type": text_type,
            "comment": None,
        },
        col_case("ip_address"): {
            "name": col_case("ip_address"),
            "index": AnyInteger(),
            "type": text_type,
            "comment": None,
        },
        col_case("updated_at"): {
            "name": col_case("updated_at"),
            "index": AnyInteger(),
            "type": time_type,
            "comment": None,
        },
    }

    return {
        "nodes": {
            "model.test.model": {
                "unique_id": "model.test.model",
                "metadata": {
                    "schema": my_schema_name,
                    "database": model_database,
                    "name": case("model"),
                    "type": view_type,
                    "comment": None,
                    "owner": role,
                },
                "stats": model_stats,
                "columns": expected_cols,
            },
            "model.test.second_model": {
                "unique_id": "model.test.second_model",
                "metadata": {
                    "schema": alternate_schema,
                    "database": project.database,
                    "name": case("second_model"),
                    "type": view_type,
                    "comment": None,
                    "owner": role,
                },
                "stats": model_stats,
                "columns": expected_cols,
            },
            "seed.test.seed": {
                "unique_id": "seed.test.seed",
                "metadata": {
                    "schema": my_schema_name,
                    "database": project.database,
                    "name": case("seed"),
                    "type": table_type,
                    "comment": None,
                    "owner": role,
                },
                "stats": seed_stats,
                "columns": expected_cols,
            },
        },
        "sources": {
            "source.test.my_source.my_table": {
                "unique_id": "source.test.my_source.my_table",
                "metadata": {
                    "schema": my_schema_name,
                    "database": project.database,
                    "name": case("seed"),
                    "type": table_type,
                    "comment": None,
                    "owner": role,
                },
                "stats": seed_stats,
                "columns": expected_cols,
            },
        },
    }

class TestBaseDocsHive(BaseDocsGenerate):
    @pytest.fixture(scope="class")
    def expected_catalog(self, project, profile_user):
        return base_expected_catalog(
            project,
            role=profile_user,
            id_type="int",
            text_type="string",
            time_type="timestamp",
            view_type="view",
            table_type="table",
            model_stats=no_stats(),
            seed_stats=no_stats()
        )

def expected_references_catalog(
    project,
    role,
    id_type,
    text_type,
    time_type,
    view_type,
    table_type,
    model_stats,
    bigint_type=None,
    seed_stats=None,
    case=None,
    case_columns=False,
    view_summary_stats=None,
):
    if case is None:

        def case(x):
            return x

    col_case = case if case_columns else lambda x: x

    if seed_stats is None:
        seed_stats = model_stats

    if view_summary_stats is None:
        view_summary_stats = model_stats

    model_database = project.database
    my_schema_name = case(project.test_schema)

    summary_columns = {
        "first_name": {
            "name": "first_name",
            "index": 0,
            "type": text_type,
            "comment": None,
        },
        "ct": {
            "name": "ct",
            "index": 1,
            "type": bigint_type,
            "comment": None,
        },
    }

    seed_columns = {
        "id": {
            "name": col_case("id"),
            "index": 0,
            "type": id_type,
            "comment": None,
        },
        "first_name": {
            "name": col_case("first_name"),
            "index": 1,
            "type": text_type,
            "comment": None,
        },
        "email": {
            "name": col_case("email"),
            "index": 2,
            "type": text_type,
            "comment": None,
        },
        "ip_address": {
            "name": col_case("ip_address"),
            "index": 3,
            "type": text_type,
            "comment": None,
        },
        "updated_at": {
            "name": col_case("updated_at"),
            "index": 4,
            "type": time_type,
            "comment": None,
        },
    }
    return {
        "nodes": {
            "seed.test.seed": {
                "unique_id": "seed.test.seed",
                "metadata": {
                    "schema": my_schema_name,
                    "database": project.database,
                    "name": case("seed"),
                    "type": table_type,
                    "comment": None,
                    "owner": role,
                },
                "stats": seed_stats,
                "columns": seed_columns,
            },
            "model.test.view_summary": {
                "unique_id": "model.test.view_summary",
                "metadata": {
                    "schema": my_schema_name,
                    "database": model_database,
                    "name": case("view_summary"),
                    "type": view_type,
                    "comment": None,
                    "owner": role,
                },
                "stats": view_summary_stats,
                "columns": summary_columns,
            },
        },
        "sources": {
            "source.test.my_source.my_table": {
                "unique_id": "source.test.my_source.my_table",
                "metadata": {
                    "schema": my_schema_name,
                    "database": project.database,
                    "name": case("seed"),
                    "type": table_type,
                    "comment": None,
                    "owner": role,
                },
                "stats": seed_stats,
                "columns": seed_columns,
            },
        },
    }

ref_models__schema_yml = """
version: 2

models:
  - name: table_summary
    description: "{{ doc('table_summary') }}"
    columns: &summary_columns
      - name: first_name
        description: "{{ doc('summary_first_name') }}"
      - name: ct
        description: "{{ doc('summary_count') }}"
  - name: view_summary
    description: "{{ doc('view_summary') }}"
    columns: *summary_columns

exposures:
  - name: notebook_exposure
    type: notebook
    depends_on:
      - ref('view_summary')
    owner:
      email: something@example.com
      name: Some name
    description: "{{ doc('notebook_info') }}"
    maturity: medium
    url: http://example.com/notebook/1
    meta:
      tool: 'my_tool'
      languages:
        - python
    tags: ['my_department']

"""

ref_sources__schema_yml = """
version: 2
sources:
  - name: my_source
    description: "{{ doc('source_info') }}"
    loader: a_loader
    schema: "{{ var('test_schema') }}"
    quoting:
      database: False
      identifier: False
    tables:
      - name: my_table
        description: "{{ doc('table_info') }}"
        identifier: seed
        quoting:
          identifier: False
        columns:
          - name: id
            description: "{{ doc('column_info') }}"
"""

ref_models__view_summary_sql = """
{{
  config(
    materialized = "view"
  )
}}

select first_name, ct from {{ref('table_summary')}}
order by ct asc

"""

ref_models__table_summary_sql = """
{{
  config(
    materialized = "table"
  )
}}

select first_name, count(*) as ct from {{ref('table_copy')}}
group by first_name
order by first_name asc

"""

ref_models__table_copy_sql = """
{{
  config(
    materialized = "table"
  )
}}

select * from {{ source("my_source", "my_table") }}

"""

ref_models__docs_md = """
{% docs table_summary %}
A summmary table of the table copy of the seed data
{% enddocs %}

{% docs summary_first_name %}
The first name being summarized
{% enddocs %}

{% docs summary_count %}
The number of instances of the first name
{% enddocs %}

{% docs view_summary %}
A view of the summary of the table copy of the seed data
{% enddocs %}

{% docs source_info %}
My source
{% enddocs %}

{% docs table_info %}
My table
{% enddocs %}

{% docs column_info %}
An ID field
{% enddocs %}

{% docs notebook_info %}
A description of the complex exposure
{% enddocs %}

"""

def run_and_generate(project, args=None):
    results = run_dbt(["run"])
    rm_file(project.project_root, "target", "manifest.json")
    rm_file(project.project_root, "target", "run_results.json")

    start_time = datetime.utcnow()
    run_args = ["docs", "generate"]
    if args:
        run_args.extend(args)
    catalog = run_dbt(run_args)
    assert catalog
    return start_time

class TestBaseDocsGenRefsHive(BaseDocsGenReferences):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": ref_models__schema_yml,
            "sources.yml": ref_sources__schema_yml,
            "table_summary.sql": ref_models__table_summary_sql,
            "table_copy.sql": ref_models__table_copy_sql,
            "view_summary.sql": ref_models__view_summary_sql,
            "docs.md": ref_models__docs_md,
        }

    @pytest.fixture(scope="class")
    def expected_catalog(self, project, profile_user):
        return expected_references_catalog(
            project,
            role=profile_user,
            id_type="bigint",
            text_type="string",
            time_type="timestamp",
            view_type="view",
            table_type="table",
            model_stats=no_stats(),
            seed_stats=no_stats(),
            bigint_type="bigint"
        )

    def test_references(self, project, expected_catalog):
        start_time = run_and_generate(project)
        verify_catalog(project, expected_catalog, start_time)
