import pytest

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import (
    BaseIncremental,
    BaseIncrementalNotSchemaChange,
)
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.utils.base_utils import BaseUtils
from dbt.tests.util import (
    run_dbt,
    check_result_nodes_by_name,
    relation_from_name,
    check_relation_types,
    check_relations_equal,
)
from dbt.tests.adapter.basic.files import (
    base_table_sql,
    base_materialized_var_sql,
    schema_base_yml,
)

from tests.functional.adapter.config_files import insertoverwrite_sql


class TestSimpleMaterializationsHive(BaseSimpleMaterializations):
    """Modified Simple Materialization Hive test.
    This test has been overridden, because of hive bug
    https://jira.cloudera.com/browse/CDPD-20768 i.e. show tables statements are not
    working as expected in hive.
    Hence, dbt-hive adapter cannot check if the given entity is table or view and
    default assume them tables.
    Once, the original issue is fixed, please remove the overriden behavior.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model.sql": base_table_sql,
            "swappable.sql": base_materialized_var_sql,
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
        assert len(results) == 2

        # names exist in result nodes
        check_result_nodes_by_name(results, ["table_model", "swappable"])

        # check relation types
        expected = {
            "base": "table",
            "table_model": "table",
            "swappable": "table",
        }
        check_relation_types(project.adapter, expected)

        # base table rowcount
        relation = relation_from_name(project.adapter, "base")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10

        # relations_equal
        check_relations_equal(project.adapter, ["base", "table_model", "swappable"])

        # check relations in catalog
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 3
        assert len(catalog.sources) == 1

        # run_dbt changing materialized_var to incremental
        results = run_dbt(["run", "-m", "swappable", "--vars", "materialized_var: incremental"])
        assert len(results) == 1

        # check relation types, swappable is table
        expected = {
            "base": "table",
            "table_model": "table",
            "swappable": "table",
        }
        check_relation_types(project.adapter, expected)


class TestSingularTestsHive(BaseSingularTests):
    pass


class TestEmptyHive(BaseEmpty):
    pass


class TestEphemeralHive(BaseEphemeral):
    pass


class TestGenericTestsHive(BaseGenericTests):
    pass


class TestBaseAdapterMethod(BaseAdapterMethod):
    pass


class TestBaseUtilsHive(BaseUtils):
    pass


class TestIncrementalHive(BaseIncremental):
    pass


class TestIncrementalFullRefreshHive(BaseIncremental):
    def test_incremental(self, project):
        # seed command
        results = run_dbt(["seed"])

        # run command
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["run", "--vars", "seed_name: base"])
        assert len(results) == 1

        # check relations equal
        check_relations_equal(project.adapter, ["base", "incremental"])

        # run full-refresh and compare with base table again
        results = run_dbt(
            ["run", "--select", "incremental", "--full-refresh", "--vars", "seed_name: base"]
        )
        assert len(results) == 1

        check_relations_equal(project.adapter, ["base", "incremental"])


incremental_single_partitionby_sql = """
 {{ config(materialized="incremental", partition_by="id_partition") }}
 select
    *, id as id_partition from {{ source('raw', 'seed') }}
    {% if is_incremental() %}
        where id > (select max(id) from {{ this }})
    {% endif %}
""".strip()


class TestIncrementalPartitionHive(BaseIncremental):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental.sql": incremental_single_partitionby_sql,
            "schema.yml": schema_base_yml,
        }


incremental_multiple_partitionby_sql = """
 {{ config(materialized="incremental", partition_by=["id_partition1", "id_partition2"]) }}
 select
    *, id as id_partition1, id as id_partition2 from {{ source('raw', 'seed') }}
    {% if is_incremental() %}
        where id > (select max(id) from {{ this }})
    {% endif %}
""".strip()


class TestIncrementalMultiplePartitionsHive(BaseIncremental):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental.sql": incremental_multiple_partitionby_sql,
            "schema.yml": schema_base_yml,
        }


incremental_not_schema_change_sql = """
{{ config(materialized="incremental", incremental_strategy="append") }}
select
    concat(concat('1', '-'), cast(current_timestamp() as string)) as user_id_current_time,
    {% if is_incremental() %}
        'thisis18characters' as platform
    {% else %}
        'okthisis20characters' as platform
    {% endif %}
"""


class TestInsertOverwriteHive(BaseIncremental):
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental.sql": insertoverwrite_sql, "schema.yml": schema_base_yml}


class TestBaseIncrementalNotSchemaChange(BaseIncrementalNotSchemaChange):
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_not_schema_change.sql": incremental_not_schema_change_sql}
