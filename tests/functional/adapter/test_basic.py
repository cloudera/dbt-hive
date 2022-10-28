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
# 
# 
class TestIncrementalHive(BaseIncremental):
   pass
# 
# 
class TestGenericTestsHive(BaseGenericTests):
   pass
# 
# NG 
# class TestSnapshotCheckColsHive(BaseSnapshotCheckCols):
#    pass
# 
# NG
# class TestSnapshotTimestampHive(BaseSnapshotTimestamp):
#     pass
# 
#

class TestBaseAdapterMethod(BaseAdapterMethod):
    pass

class TestBaseDocsHive(BaseDocsGenerate):
     pass

class TestBaseDocsGenRefsHive(BaseDocsGenReferences):
     pass

class TestBaseUtilsHive(BaseUtils):
     pass
