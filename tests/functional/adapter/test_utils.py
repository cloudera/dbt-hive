import pytest
from dbt.tests.adapter.utils.base_utils import BaseUtils
from dbt.tests.adapter.utils.test_any_value import BaseAnyValue
from dbt.tests.adapter.utils.test_bool_or import BaseBoolOr
from dbt.tests.adapter.utils.test_cast_bool_to_text import BaseCastBoolToText
from dbt.tests.adapter.utils.test_concat import BaseConcat
from dbt.tests.adapter.utils.test_dateadd import BaseDateAdd
from dbt.tests.adapter.utils.test_datediff import BaseDateDiff
from dbt.tests.adapter.utils.test_date_trunc import BaseDateTrunc
from dbt.tests.adapter.utils.test_escape_single_quotes import BaseEscapeSingleQuotesQuote
from dbt.tests.adapter.utils.test_escape_single_quotes import BaseEscapeSingleQuotesBackslash
from dbt.tests.adapter.utils.test_except import BaseExcept
from dbt.tests.adapter.utils.test_hash import BaseHash
from dbt.tests.adapter.utils.test_intersect import BaseIntersect
from dbt.tests.adapter.utils.test_last_day import BaseLastDay
from dbt.tests.adapter.utils.test_length import BaseLength
from dbt.tests.adapter.utils.test_listagg import BaseListagg
from dbt.tests.adapter.utils.test_position import BasePosition
from dbt.tests.adapter.utils.test_replace import BaseReplace
from dbt.tests.adapter.utils.test_right import BaseRight
from dbt.tests.adapter.utils.test_safe_cast import BaseSafeCast
from dbt.tests.adapter.utils.test_split_part import BaseSplitPart
from dbt.tests.adapter.utils.test_string_literal import BaseStringLiteral

from dbt.tests.adapter.utils.fixture_any_value import (
    seeds__data_any_value_csv,
    seeds__data_any_value_expected_csv,
    models__test_any_value_sql,
    models__test_any_value_yml,
)

from dbt.tests.adapter.utils.fixture_bool_or import (
    seeds__data_bool_or_csv,
    seeds__data_bool_or_expected_csv,
    models__test_bool_or_sql,
    models__test_bool_or_yml,
)

from dbt.tests.adapter.utils.fixture_cast_bool_to_text import (
    models__test_cast_bool_to_text_sql,
    models__test_cast_bool_to_text_yml,
)

from dbt.tests.adapter.utils.fixture_concat import (
    seeds__data_concat_csv,
    models__test_concat_sql,
    models__test_concat_yml,
)

from dbt.tests.adapter.utils.fixture_dateadd import (
    seeds__data_dateadd_csv,
    models__test_dateadd_sql,
    models__test_dateadd_yml,
)

from dbt.tests.adapter.utils.fixture_datediff import (
    seeds__data_datediff_csv,
    models__test_datediff_sql,
    models__test_datediff_yml,
)

from dbt.tests.adapter.utils.fixture_date_trunc import (
    seeds__data_date_trunc_csv,
    models__test_date_trunc_sql,
    models__test_date_trunc_yml,
)

from dbt.tests.adapter.utils.fixture_hash import (
    seeds__data_hash_csv,
    models__test_hash_sql,
    models__test_hash_yml,
)

from dbt.tests.adapter.utils.fixture_last_day import (
    seeds__data_last_day_csv,
    models__test_last_day_sql,
    models__test_last_day_yml,
)

from dbt.tests.adapter.utils.fixture_length import (
    seeds__data_length_csv,
    models__test_length_sql,
    models__test_length_yml,
)

from dbt.tests.adapter.utils.fixture_listagg import (
    seeds__data_listagg_csv,
    seeds__data_listagg_output_csv,
    models__test_listagg_sql,
    models__test_listagg_yml,
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

from dbt.tests.adapter.utils.fixture_safe_cast import (
    seeds__data_safe_cast_csv,
    models__test_safe_cast_sql,
    models__test_safe_cast_yml,
)

from dbt.tests.adapter.utils.fixture_split_part import (
    seeds__data_split_part_csv,
    models__test_split_part_sql,
    models__test_split_part_yml,
)

models__test_any_value_sql = """
with util_data as (

    select * from {{ ref('data_any_value') }}

),

data_output as (

    select * from {{ ref('data_any_value_expected') }}

),

calculate as (
    select
        key_name,
        {{ any_value('static_col') }} as static_col,
        count(id) as num_rows
    from util_data
    group by key_name
)

select
    calculate.num_rows as actual,
    data_output.num_rows as expected
from calculate
left join data_output
on calculate.key_name = data_output.key_name
and calculate.static_col = data_output.static_col
"""

class TestAnyValue(BaseAnyValue):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_any_value.csv": seeds__data_any_value_csv,
            "data_any_value_expected.csv": seeds__data_any_value_expected_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_any_value.yml": models__test_any_value_yml,
            "test_any_value.sql": self.interpolate_macro_namespace(
                models__test_any_value_sql, "any_value"
            ),
        }

models__test_bool_or_sql = """
with util_data as (

    select * from {{ ref('data_bool_or') }}

),

data_output as (

    select * from {{ ref('data_bool_or_expected') }}

),

calculate as (

    select
        key,
        {{ bool_or('val1 = val2') }} as value
    from util_data
    group by key

)

select
    calculate.value as actual,
    data_output.value as expected
from calculate
left join data_output
on calculate.key = data_output.key
"""

class TestBoolOr(BaseBoolOr):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_bool_or.csv": seeds__data_bool_or_csv,
            "data_bool_or_expected.csv": seeds__data_bool_or_expected_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_bool_or.yml": models__test_bool_or_yml,
            "test_bool_or.sql": self.interpolate_macro_namespace(
                models__test_bool_or_sql, "bool_or"
            ),
        }

models__test_cast_bool_to_text_sql = """
with util_data as (

    select 0=1 as input, 'false' as expected union all
    select 1=1 as input, 'true' as expected union all
    select null as input, null as expected

)

select

    {{ cast_bool_to_text("input") }} as actual,
    expected

from util_data
"""

class TestCastBoolToText(BaseCastBoolToText):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_cast_bool_to_text.yml": models__test_cast_bool_to_text_yml,
            "test_cast_bool_to_text.sql": self.interpolate_macro_namespace(
                models__test_cast_bool_to_text_sql, "cast_bool_to_text"
            ),
        }


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

models__test_dateadd_sql = """
with util_data as (

    select * from {{ ref('data_dateadd') }}

)

select
    case
        when datepart = 'hour' then cast({{ dateadd('hour', 'interval_length', 'from_time') }} as {{ api.Column.translate_type('timestamp') }})
        when datepart = 'day' then cast({{ dateadd('day', 'interval_length', 'from_time') }} as {{ api.Column.translate_type('timestamp') }})
        when datepart = 'month' then cast({{ dateadd('month', 'interval_length', 'from_time') }} as {{ api.Column.translate_type('timestamp') }})
        when datepart = 'year' then cast({{ dateadd('year', 'interval_length', 'from_time') }} as {{ api.Column.translate_type('timestamp') }})
        else null
    end as actual,
    result as expected

from util_data
"""

class TestDateAdd(BaseDateAdd):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test",
            # this is only needed for BigQuery, right?
            # no harm having it here until/unless there's an adapter that doesn't support the 'timestamp' type
            "seeds": {
                "test": {
                    "data_dateadd": {
                        "+column_types": {
                            "from_time": "timestamp",
                            "result": "timestamp",
                        },
                    },
                },
            },
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_dateadd.csv": seeds__data_dateadd_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_dateadd.yml": models__test_dateadd_yml,
            "test_dateadd.sql": self.interpolate_macro_namespace(
                models__test_dateadd_sql, "dateadd"
            ),
        }

models__test_datediff_sql = """
with util_data as (

    select * from {{ ref('data_datediff') }}

)

select

    case
        when datepart = 'second' then {{ datediff('first_date', 'second_date', 'second') }}
        when datepart = 'minute' then {{ datediff('first_date', 'second_date', 'minute') }}
        when datepart = 'hour' then {{ datediff('first_date', 'second_date', 'hour') }}
        when datepart = 'day' then {{ datediff('first_date', 'second_date', 'day') }}
        when datepart = 'week' then {{ datediff('first_date', 'second_date', 'week') }}
        when datepart = 'month' then {{ datediff('first_date', 'second_date', 'month') }}
        when datepart = 'year' then {{ datediff('first_date', 'second_date', 'year') }}
        else null
    end as actual,
    result as expected

from util_data

-- Also test correct casting of literal values.

union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-01 00:00:00.000000'", "microsecond") }} as actual, 1 as expected
union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-01 00:00:00.000000'", "millisecond") }} as actual, 1 as expected
union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-01 00:00:00.000000'", "second") }} as actual, 1 as expected
union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-01 00:00:00.000000'", "minute") }} as actual, 1 as expected
union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-01 00:00:00.000000'", "hour") }} as actual, 1 as expected
union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-01 00:00:00.000000'", "day") }} as actual, 1 as expected
union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-03 00:00:00.000000'", "week") }} as actual, 1 as expected
union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-01 00:00:00.000000'", "month") }} as actual, 1 as expected
union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-01 00:00:00.000000'", "quarter") }} as actual, 1 as expected
union all select {{ datediff("'1999-12-31 23:59:59.999999'", "'2000-01-01 00:00:00.000000'", "year") }} as actual, 1 as expected
"""

class TestDateDiff(BaseDateDiff):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_datediff.csv": seeds__data_datediff_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_datediff.yml": models__test_datediff_yml,
            "test_datediff.sql": self.interpolate_macro_namespace(
                models__test_datediff_sql, "datediff"
            ),
        }

models__test_date_trunc_sql = """
with util_data as (

    select * from {{ ref('data_date_trunc') }}

)

select
    cast({{date_trunc('day', 'updated_at') }} as date) as actual,
    day as expected

from data

union all

select
    cast({{ date_trunc('month', 'updated_at') }} as date) as actual,
    month as expected

from util_data
"""
class TestDateTrunc(BaseDateTrunc):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_date_trunc.csv": seeds__data_date_trunc_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_date_trunc.yml": models__test_date_trunc_yml,
            "test_date_trunc.sql": self.interpolate_macro_namespace(
                models__test_date_trunc_sql, "date_trunc"
            ),
        }


class TestEscapeSingleQuotes(BaseEscapeSingleQuotesQuote):
    pass


class TestExcept(BaseExcept):
    pass


models__test_hash_sql = """
with util_data as (

    select * from {{ ref('data_hash') }}

)

select
    {{ hash('input_1') }} as actual,
    output as expected

from util_data
"""
class TestHash(BaseHash):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_hash.csv": seeds__data_hash_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_hash.yml": models__test_hash_yml,
            "test_hash.sql": self.interpolate_macro_namespace(models__test_hash_sql, "hash"),
        }


class TestIntersect(BaseIntersect):
    pass


models__test_last_day_sql = """
with util_data as (

    select * from {{ ref('data_last_day') }}

)

select
    case
        when date_part = 'month' then {{ last_day('date_day', 'month') }}
        when date_part = 'quarter' then {{ last_day('date_day', 'quarter') }}
        when date_part = 'year' then {{ last_day('date_day', 'year') }}
        else null
    end as actual,
    result as expected

from util_data
"""
class TestLastDay(BaseLastDay):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_last_day.csv": seeds__data_last_day_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_last_day.yml": models__test_last_day_yml,
            "test_last_day.sql": self.interpolate_macro_namespace(
                models__test_last_day_sql, "last_day"
            ),
        }


models__test_length_sql = """
with util_data as (

    select * from {{ ref('data_length') }}

)

select

    {{ length('expression') }} as actual,
    output as expected

from util_data
"""
class TestLength(BaseLength):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_length.csv": seeds__data_length_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_length.yml": models__test_length_yml,
            "test_length.sql": self.interpolate_macro_namespace(models__test_length_sql, "length"),
        }

models__test_listagg_sql = """
with util_data as (

    select * from {{ ref('data_listagg') }}

),

data_output as (

    select * from {{ ref('data_listagg_output') }}

),

calculate as (

    select
        group_col,
        {{ listagg('string_text', "'_|_'", "order by order_col") }} as actual,
        'bottom_ordered' as version
    from util_data
    group by group_col

    union all

    select
        group_col,
        {{ listagg('string_text', "'_|_'", "order by order_col", 2) }} as actual,
        'bottom_ordered_limited' as version
    from util_data
    group by group_col

    union all

    select
        group_col,
        {{ listagg('string_text', "', '") }} as actual,
        'comma_whitespace_unordered' as version
    from util_data
    where group_col = 3
    group by group_col

    union all

    select
        group_col,
        {{ listagg('DISTINCT string_text', "','") }} as actual,
        'distinct_comma' as version
    from util_data
    where group_col = 3
    group by group_col

    union all

    select
        group_col,
        {{ listagg('string_text') }} as actual,
        'no_params' as version
    from util_data
    where group_col = 3
    group by group_col

)

select
    calculate.actual,
    data_output.expected
from calculate
left join data_output
on calculate.group_col = data_output.group_col
and calculate.version = data_output.version
"""
class TestListagg(BaseListagg):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_listagg.csv": seeds__data_listagg_csv,
            "data_listagg_output.csv": seeds__data_listagg_output_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_listagg.yml": models__test_listagg_yml,
            "test_listagg.sql": self.interpolate_macro_namespace(
                models__test_listagg_sql, "listagg"
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


models__test_safe_cast_sql = """
with util_data as (

    select * from {{ ref('data_safe_cast') }}

)

select
    {{ safe_cast('field', api.Column.translate_type('string')) }} as actual,
    output as expected

from util_data
"""
class TestSafeCast(BaseSafeCast):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_safe_cast.csv": seeds__data_safe_cast_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_safe_cast.yml": models__test_safe_cast_yml,
            "test_safe_cast.sql": self.interpolate_macro_namespace(
                self.interpolate_macro_namespace(models__test_safe_cast_sql, "safe_cast"),
                "type_string",
            ),
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

