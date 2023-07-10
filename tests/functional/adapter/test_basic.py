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


class TestBaseIncrementalNotSchemaChange(BaseIncrementalNotSchemaChange):
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_not_schema_change.sql": incremental_not_schema_change_sql}
