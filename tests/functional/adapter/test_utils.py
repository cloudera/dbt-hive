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
import pytest
from dbt.tests.adapter.utils.base_utils import BaseUtils
from dbt.tests.adapter.utils.test_any_value import BaseAnyValue
from dbt.tests.adapter.utils.test_concat import BaseConcat
from dbt.tests.adapter.utils.test_escape_single_quotes import BaseEscapeSingleQuotesQuote
from dbt.tests.adapter.utils.test_escape_single_quotes import BaseEscapeSingleQuotesBackslash
from dbt.tests.adapter.utils.test_position import BasePosition
from dbt.tests.adapter.utils.test_replace import BaseReplace
from dbt.tests.adapter.utils.test_right import BaseRight
from dbt.tests.adapter.utils.test_split_part import BaseSplitPart
from dbt.tests.adapter.utils.test_string_literal import BaseStringLiteral


from dbt.tests.adapter.utils.fixture_concat import (
    seeds__data_concat_csv,
    models__test_concat_sql,
    models__test_concat_yml,
)

from dbt.tests.adapter.utils.fixture_length import (
    seeds__data_length_csv,
    models__test_length_sql,
    models__test_length_yml,
)

from dbt.tests.adapter.utils.fixture_position import (
    seeds__data_position_csv,
    models__test_position_sql,
    models__test_position_yml,
)

from dbt.tests.adapter.utils.fixture_replace import (
    seeds__data_replace_csv,
    models__test_replace_sql,
    models__test_replace_yml,
)

from dbt.tests.adapter.utils.fixture_right import (
    seeds__data_right_csv,
    models__test_right_sql,
    models__test_right_yml,
)

from dbt.tests.adapter.utils.fixture_split_part import (
    seeds__data_split_part_csv,
    models__test_split_part_sql,
    models__test_split_part_yml,
)

from dbt.tests.adapter.utils.fixture_escape_single_quotes import (
     models__test_escape_single_quotes_quote_sql,
     models__test_escape_single_quotes_yml,
)


models__test_concat_sql = """
with util_data as (
    select * from {{ ref('data_concat') }}
)
select
    {{ concat(['input_1', 'input_2']) }} as actual,
    output as expected
from util_data
"""
class TestConcat(BaseConcat):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_concat.csv": seeds__data_concat_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_concat.yml": models__test_concat_yml,
            "test_concat.sql": self.interpolate_macro_namespace(models__test_concat_sql, "concat"),
        }


models__test_escape_single_quotes_quote_sql = """
 select '{{ escape_single_quotes("they're") }}' as actual, 'they\\'re' as expected union all
 select '{{ escape_single_quotes("they are") }}' as actual, 'they are' as expected
"""

class TestEscapeSingleQuotes(BaseEscapeSingleQuotesQuote):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_escape_single_quotes.yml": models__test_escape_single_quotes_yml,
            "test_escape_single_quotes.sql": self.interpolate_macro_namespace(
                models__test_escape_single_quotes_quote_sql, "escape_single_quotes"
            ),
        }


models__test_position_sql = """
with util_data as (
    select * from {{ ref('data_position') }}
)
select
    {{ position('substring_text', 'string_text') }} as actual,
    result as expected
from util_data
"""
class TestPosition(BasePosition):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_position.csv": seeds__data_position_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_position.yml": models__test_position_yml,
            "test_position.sql": self.interpolate_macro_namespace(
                models__test_position_sql, "position"
            ),
        }

models__test_replace_sql = """
with util_data as (
    select
        *,
        coalesce(search_chars, '') as old_chars,
        coalesce(replace_chars, '') as new_chars
    from {{ ref('data_replace') }}
)
select
    {{ replace('string_text', 'old_chars', 'new_chars') }} as actual,
    result as expected
from util_data
"""
class TestReplace(BaseReplace):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_replace.csv": seeds__data_replace_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_replace.yml": models__test_replace_yml,
            "test_replace.sql": self.interpolate_macro_namespace(
                models__test_replace_sql, "replace"
            ),
        }

models__test_right_sql = """
with util_data as (
    select * from {{ ref('data_right') }}
)
select
    {{ right('string_text', 'length_expression') }} as actual,
    coalesce(output, '') as expected
from util_data
"""
class TestRight(BaseRight):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_right.csv": seeds__data_right_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_right.yml": models__test_right_yml,
            "test_right.sql": self.interpolate_macro_namespace(models__test_right_sql, "right"),
        }


models__test_split_part_sql = """
with util_data as (
    select * from {{ ref('data_split_part') }}
)
select
    {{ split_part('parts', 'split_on', 1) }} as actual,
    result_1 as expected
from util_data
union all
select
    {{ split_part('parts', 'split_on', 2) }} as actual,
    result_2 as expected
from util_data
union all
select
    {{ split_part('parts', 'split_on', 3) }} as actual,
    result_3 as expected
from util_data
"""
class TestSplitPart(BaseSplitPart):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_split_part.csv": seeds__data_split_part_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_split_part.yml": models__test_split_part_yml,
            "test_split_part.sql": self.interpolate_macro_namespace(
                models__test_split_part_sql, "split_part"
            ),
        }


class TestStringLiteral(BaseStringLiteral):
    pass
