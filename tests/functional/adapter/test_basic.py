import pytest

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.utils.base_utils import BaseUtils

class TestSimpleMaterializationsHive(BaseSimpleMaterializations):
   pass


class TestSingularTestsHive(BaseSingularTests):
   pass


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
        
class TestBaseUtilsHive(BaseUtils):
     pass
