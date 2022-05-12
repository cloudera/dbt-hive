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

class TestSimpleMaterializationsMyAdapter(BaseSimpleMaterializations):
   pass


class TestSingularTestsMyAdapter(BaseSingularTests):
   pass


# NG
# ephemeral with CTE does not work with hive
# class TestSingularTestsEphemeralMyAdapter(BaseSingularTestsEphemeral):
#   pass

class TestEmptyMyAdapter(BaseEmpty):
   pass


class TestEphemeralMyAdapter(BaseEphemeral):
   pass
# 
# 
class TestIncrementalMyAdapter(BaseIncremental):
   pass
# 
# 
class TestGenericTestsMyAdapter(BaseGenericTests):
   pass
# 
# NG 
# class TestSnapshotCheckColsMyAdapter(BaseSnapshotCheckCols):
#    pass
# 
# NG
# class TestSnapshotTimestampMyAdapter(BaseSnapshotTimestamp):
#     pass
# 
#

class TestBaseAdapterMethod(BaseAdapterMethod):
    pass
