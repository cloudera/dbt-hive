import pytest

from dbt.tests.util import run_dbt, check_relations_equal
from dbt.tests.util import run_dbt, check_result_nodes_by_name

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import BaseSingularTestsEphemeral
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_docs_generate import BaseDocsGenerate, BaseDocsGenReferences
from dbt.tests.adapter.utils.base_utils import BaseUtils

from dbt.tests.adapter.basic.expected_catalog import base_expected_catalog, no_stats

class TestSimpleMaterializationsHive(BaseSimpleMaterializations):
   pass


class TestSingularTestsHive(BaseSingularTests):
   pass


# NG
# ephemeral with CTE does not work with hive
# class TestSingularTestsEphemeralHive(BaseSingularTestsEphemeral):
#   pass

class TestEmptyHive(BaseEmpty):
   pass


class TestEphemeralHive(BaseEphemeral):
   pass

class TestIncrementalHive(BaseIncremental):
   pass


class TestGenericTestsHive(BaseGenericTests):
   pass

class TestBaseAdapterMethod(BaseAdapterMethod):
    pass

class TestBaseDocsHive(BaseDocsGenerate):
    @pytest.fixture(scope="class")
    def expected_catalog(self, project, profile_user):
        return base_expected_catalog(
            project,
            role=profile_user,
            id_type="integer",
            text_type="text",
            time_type="timestamp",
            view_type="view",
            table_type="table",
            model_stats=no_stats(),
            seed_stats=no_stats()
        )

class TestBaseDocsGenRefsHive(BaseDocsGenReferences):
    @pytest.fixture(scope="class")
    def expected_catalog(self, project, profile_user):
        return base_expected_catalog(
            project,
            role=profile_user,
            id_type="integer",
            text_type="text",
            time_type="timestamp",
            view_type="view",
            table_type="table",
            model_stats=no_stats(),
            seed_stats=no_stats()
        )
        
class TestBaseUtilsHive(BaseUtils):
     pass
