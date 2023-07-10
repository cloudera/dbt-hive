import pytest
from dbt.tests.adapter.utils.data_types.test_type_bigint import BaseTypeBigInt
from dbt.tests.adapter.utils.data_types.test_type_float import (
    BaseTypeFloat,
    seeds__expected_csv as seeds__float_expected_csv,
)
from dbt.tests.adapter.utils.data_types.test_type_int import (
    BaseTypeInt,
    seeds__expected_csv as seeds__int_expected_csv,
)
from dbt.tests.adapter.utils.data_types.test_type_numeric import BaseTypeNumeric
from dbt.tests.adapter.utils.data_types.test_type_string import BaseTypeString
from dbt.tests.adapter.utils.data_types.test_type_timestamp import BaseTypeTimestamp


class TestTypeBigInt(BaseTypeBigInt):
    pass


# need to explicitly cast this to avoid it being inferred/loaded as a DOUBLE in Hive
# the two are equivalent for `=` comparison, but distinct for EXCEPT comparison
seeds__float_expected_yml = """
version: 2
seeds:
  - name: expected
    config:
      column_types:
        float_col: float
"""


class TestTypeFloat(BaseTypeFloat):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected.csv": seeds__float_expected_csv,
            "expected.yml": seeds__float_expected_yml,
        }


# need to explicitly cast this to avoid it being inferred/loaded as a BIGINT in Hive
seeds__int_expected_yml = """
version: 2
seeds:
  - name: expected
    config:
      column_types:
        int_col: int
"""


class TestTypeInt(BaseTypeInt):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected.csv": seeds__int_expected_csv,
            "expected.yml": seeds__int_expected_yml,
        }


seeds__expected_csv = """numeric_col
1.2345
""".lstrip()

# need to explicitly cast this to avoid it being a double/float
seeds__expected_yml = """
version: 2
seeds:
  - name: expected
    config:
      column_types:
        numeric_col: {}
"""

models__actual_sql = """
select cast('1.2345' as double) as numeric_col
"""


class TestTypeNumeric(BaseTypeNumeric):
    def numeric_fixture_type(self):
        return "double"

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected.csv": seeds__expected_csv,
            "expected.yml": seeds__expected_yml.format(self.numeric_fixture_type()),
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"actual.sql": self.interpolate_macro_namespace(models__actual_sql, "type_numeric")}


class TestTypeString(BaseTypeString):
    pass


class TestTypeTimestamp(BaseTypeTimestamp):
    pass
