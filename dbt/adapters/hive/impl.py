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
import re
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Union, Iterable
import agate
from dbt.contracts.relation import RelationType

import dbt
import dbt.exceptions

from dbt.adapters.base import AdapterConfig
from dbt.adapters.base.impl import catch_as_completed
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.hive import HiveConnectionManager
from dbt.adapters.hive import HiveRelation
from dbt.adapters.hive import HiveColumn
from dbt.adapters.base import BaseRelation
from dbt.clients.agate_helper import DEFAULT_TYPE_TESTER
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import executor

import time
from dbt.clients import agate_helper

GET_COLUMNS_IN_RELATION_MACRO_NAME = 'get_columns_in_relation'
LIST_SCHEMAS_MACRO_NAME = 'list_schemas'

LIST_RELATIONS_MACRO_NAME = 'list_relations_without_caching'

DROP_RELATION_MACRO_NAME = 'drop_relation'
FETCH_TBL_PROPERTIES_MACRO_NAME = 'fetch_tbl_properties'
GET_CATALOG_MACRO_NAME = 'get_catalog'

KEY_TABLE_INFORMATION = '# Detailed Table Information'
KEY_TABLE_OWNER = 'Owner:'
KEY_TABLE_TYPE = 'Table Type:'

@dataclass
class HiveConfig(AdapterConfig):
    file_format: str = 'parquet'
    location_root: Optional[str] = None
    partition_by: Optional[Union[List[str], str]] = None
    clustered_by: Optional[Union[List[str], str]] = None
    buckets: Optional[int] = None
    options: Optional[Dict[str, str]] = None
    merge_update_columns: Optional[str] = None


class HiveAdapter(SQLAdapter):
    """ Hive adaptoer
        Could not use information_schema table to retrieve metadata
    """
    Relation = HiveRelation
    Column = HiveColumn
    ConnectionManager = HiveConnectionManager
    AdapterSpecificConfigs = HiveConfig


    # @classmethod
    def date_function(cls) -> str:
        return 'current_timestamp()'

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        return "string"

    @classmethod
    def convert_number_type(cls, agate_table, col_idx):
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "double" if decimals else "bigint"

    @classmethod
    def convert_date_type(cls, agate_table, col_idx):
        return "date"

    @classmethod
    def convert_time_type(cls, agate_table, col_idx):
        return "time"

    @classmethod
    def convert_datetime_type(cls, agate_table, col_idx):
        return "timestamp"

    def quote(self, identifier):
        return '`{}`'.format(identifier)


    def add_schema_to_cache(self, schema) -> str:
        """Cache a new schema in dbt. It will show up in `list relations`."""
        if schema is None:
            name = self.nice_connection_name()
            dbt.exceptions.raise_compiler_error(
                'Attempted to cache a null schema for {}'.format(name)
            )
        if dbt.flags.USE_CACHE:
            self.cache.add_schema(None, schema)
        # so jinja doesn't render things
        return ''


#     def get_relation_type( self, relation):
#         """ Get type of a relation
#         """
#         result =  None
#         kwargs = {'relation': relation}
#         rows = self.execute_macro('hive__get_columns_in_relation',kwargs=kwargs)
#         for row in rows:
#             if not row['col_name']: continue
#             if row['col_name'].strip() == KEY_TABLE_TYPE:
#                 if row['data_type'].strip() == 'VIRTUAL_VIEW': result = 'view'
#                 if row['data_type'].strip() == 'MANAGED_TABLE': result = 'table'
#                 break
#         return result


    def list_relations_without_caching(
        self, schema_relation: HiveRelation
    ) -> List[HiveRelation]:
        """ Get a list of Relation(table or view) by SQL directly
            Use different SQL statement for view/table
        """
        kwargs = {'schema': schema_relation}
        try:
            result_tables = self.execute_macro(
                'hive__list_tables_without_caching',
                kwargs=kwargs
            )
            result_views = self.execute_macro(
                'hive__list_views_without_caching',
                kwargs=kwargs
            )
        except dbt.exceptions.RuntimeException as e:
            errmsg = getattr(e, 'msg', '')
            if f"Database '{schema_relation}' not found" in errmsg:
                return []
            else:
                description = "Error while retrieving information about"
                logger.debug(f"{description} {schema_relation}: {e.msg}")
                return []

        # hive2
        # Collect table/view separately
        # Unfortunatly, Hive2 does not distincguish table/view 
        # Currently views are also listed in `show tables`
        # https://issues.apache.org/jira/browse/HIVE-14558
        # all_rows = result_tables
        # relations = []
        # for row in all_rows:
        #    relation_type = self.get_relation_type(f"{schema_relation}.{row['tab_name']}")
        #    relations.append(
        #         self.Relation.create(
        #            schema=schema_relation.schema,
        #            identifier=row['tab_name'],
        #            type=relation_type
        #        )
        #    )

        # in Hive 2, result_tables has table + view, result_views only has views
        # so we build a result_tables_without_view that doesnot have views
        
        result_tables_without_view = []
        for row in result_tables:
            # check if this table is view
            is_view = len(list(filter(lambda x: x['tab_name'] == row['tab_name'], result_views))) == 1
            if (not is_view): result_tables_without_view.append(row)
		
        relations = []
        for row in result_tables_without_view:
            relations.append(
                self.Relation.create(
                    schema=schema_relation.schema,
                    identifier=row['tab_name'],
                    type='table'
                )
            )
        for row in result_views:
            relations.append(
                self.Relation.create(
                    schema=schema_relation.schema,
                    identifier=row['tab_name'],
                    type='view'
                )
            )

        return relations


    def get_relation(self, database: str, schema: str, identifier: str) -> Optional[BaseRelation]:
        """  Get a Relation for own list
        """
        if not self.Relation.include_policy.database:
            database = None

        return super().get_relation(database, schema, identifier)


    @staticmethod
    def find_table_information_separator(rows: List[dict]) -> int:
        """ Returns the row number of the table information sections
        """
        pos = 0
        for row in rows:
            if row['col_name'] and row['col_name'].strip() == KEY_TABLE_INFORMATION:
                break
            pos += 1
        return pos


    def parse_describe_formatted(
            self,
            relation: Relation,
            raw_rows: List[agate.Row]
    ) -> List[HiveColumn]:
        # Convert the Row to a dict
        dict_rows = [dict(zip(row._keys, row._values)) for row in raw_rows]
        # Find the separator between the rows and the metadata provided
        # by the DESCRIBE TABLE FORMATTED statement
        pos = self.find_table_information_separator(dict_rows)

        # Remove rows that start with a hash, they are comments
        rows = [
            row for row in raw_rows[0:pos]
            if row['col_name'] and not row['col_name'].startswith('#')
        ]
        metadata = {
            col['col_name']: col['data_type'] for col in raw_rows[pos + 1:]
        }

        # raw_table_stats = metadata.get(KEY_TABLE_STATISTICS)
        # table_stats = HiveColumn.convert_table_stats(raw_table_stats)
        return [HiveColumn(
            table_database=None,
            table_schema=relation.schema,
            table_name=relation.name,
            table_type=relation.type,
            table_owner=str(metadata.get(KEY_TABLE_OWNER)),
            table_stats=None,
            column=column['col_name'],
            column_index=idx,
            dtype=column['data_type'],
        ) for idx, column in enumerate(rows)]


    @staticmethod
    def find_table_information_separator(rows: List[dict]) -> int:
        """ Find the position of table defailt information
        """
        pos = 0
        for row in rows:
            if row['col_name'] == '# Detailed Table Information':
                break
            pos += 1
        return pos



    def get_columns_in_relation(self, relation: Relation) -> List[HiveColumn]:
        """ Get columns from a Relation
        parent method is used to call DESCRIBE <tablename> statement
        dtype is used for correct quote
        """
        rows: List[agate.Row] = super().get_columns_in_relation(relation)
        columns = self.parse_describe_formatted(relation, rows)

        return columns



    def _get_columns_for_catalog(
        self, relation: HiveRelation
    ) -> Iterable[Dict[str, Any]]:
        """ Get columns for catalog. Used by get_one_catalog
        """
        # columns = self.parse_columns_from_information(relation)
        columns = self.get_columns_in_relation(relation)

        for column in columns:
            # convert HiveColumns into catalog dicts
            as_dict = column.to_column_dict()
            as_dict['column_name'] = as_dict.pop('column', None)
            as_dict['column_type'] = as_dict.pop('dtype')
            as_dict['table_database'] = None
            yield as_dict



    def get_properties(self, relation: Relation) -> Dict[str, str]:
        """
        """
        properties = self.execute_macro(
            FETCH_TBL_PROPERTIES_MACRO_NAME,
            kwargs={'relation': relation}
        )
        return dict(properties)



    def get_catalog(self, manifest):
        """ Return a catalogs that contains information of all schemas
        """
        schema_map = self._get_catalog_schemas(manifest)
        if len(schema_map) > 1:
            dbt.exceptions.raise_compiler_error(
                f'Expected only one database in get_catalog, found '
                f'{list(schema_map)}'
            )

        # run heavy job in other threads
        with executor(self.config) as tpe:
            futures: List[Future[agate.Table]] = []
            for info, schemas in schema_map.items():
                for schema in schemas:
                   futures.append(tpe.submit_connected(
                       self, schema,
                       self._get_one_catalog, info, [schema], manifest
                   ))
            # at this point all catalogs in futures list will be merged into one
            # insides`catch_on_completed` method of the parent class
            catalogs, exceptions = catch_as_completed(futures)

#         catalogs = agate.Table([])
#         exceptions = None
#         for info, schemas in schema_map.items():
#             for schema in schemas:
#                 catalog = self._get_one_catalog(info,[schema],manifest)
#                 # print('len of catalogs: {}'.format(len(catalogs)))
#                 print(catalogs)
#                 print("-------")
#                 print(catalog)
#                 # print('len of the new catalog: {}'.format(len(catalog)))
#                 catalogs = agate.Table.merge([catalogs,catalog])

        return catalogs, exceptions


    def _get_one_catalog(
        self, information_schema, schemas, manifest,
    ) -> agate.Table:
        """ Get ONE catalog. Used by get_catalog

        manifest is used to run the method in other context's
        threadself.get_columns_in_relation
        """
        if len(schemas) != 1:
            dbt.exceptions.raise_compiler_error(
                f'Expected only one schema in Hive _get_one_catalog, found '
                f'{schemas}'
            )

        database = information_schema.database
        schema = list(schemas)[0]

        schema_relation = self.Relation.create(
            database=database,
            schema=schema,
            identifier='',
            quote_policy=self.config.quoting
        ).without_identifier()

        columns: List[Dict[str, Any]] = []
        for relation in self.list_relations(database, schema):
            logger.debug("Getting table schema for relation {}", relation)
            columns.extend(self._get_columns_for_catalog(relation))

        if len(columns) > 0:
            text_types = agate_helper.build_type_tester(['table_owner','table_database'])
        else:
            text_types = []

        return agate.Table.from_object(
            columns,
            # column_types=DEFAULT_TYPE_TESTER
            column_types = text_types
        )



    def check_schema_exists(self, database, schema):
        results = self.execute_macro(
            LIST_SCHEMAS_MACRO_NAME,
            kwargs={'database': database}
        )

        exists = True if schema in [row[0] for row in results] else False
        return exists


    def get_rows_different_sql(
        self,
        relation_a: BaseRelation,
        relation_b: BaseRelation,
        column_names: Optional[List[str]] = None,
        except_operator: str = 'EXCEPT',
    ) -> str:
        """Generate SQL for a query that returns a single row with a two
        columns: the number of rows that are different between the two
        relations and the number of mismatched rows.
        """
        # This method only really exists for test reasons.
        names: List[str]
        if column_names is None:
            columns = self.get_columns_in_relation(relation_a)
            names = sorted((self.quote(c.name) for c in columns))
        else:
            names = sorted((self.quote(n) for n in column_names))
        columns_csv = ', '.join(names)

        columns_a_csv = ",".join(['t1.%s' % col for col in names])
        columns_b_csv = ",".join(['t2.%s' % col for col in names])
        join_condition_csv = " AND ".join(['t1.%s=t2.%s' % (col,col) for col in names])

        sql = COLUMNS_EQUAL_SQL.format(
            join_condition=join_condition_csv,
            columns_a=columns_a_csv,
            columns_b=columns_b_csv,
            relation_a=str(relation_a),
            relation_b=str(relation_b),
        )

        return sql


# hive does something interesting with joins when both tables have the same
# static values for the join condition and complains that the join condition is
# "trivial". Which is true, though it seems like an unreasonable cause for
# failure! It also doesn't like the `from foo, bar` syntax as opposed to
# `from foo cross join bar`.
COLUMNS_EQUAL_SQL = '''
with
overlap as (
    SELECT {columns_a}
    FROM {relation_a} t1,{relation_b} t2
    WHERE ({join_condition})
),
outliner as (
    SELECT {columns_a} FROM {relation_a} t1
    UNION
    SELECT {columns_b} FROM {relation_b} t2
),
diff as (
    SELECT {columns_a} FROM outliner t1
    LEFT JOIN overlap t2
    ON ({join_condition})
    WHERE t2.`id` IS NULL
),
diff_count as (
    SELECT
        1 as id,
        COUNT(*) as num_missing FROM diff
), table_a as (
    SELECT COUNT(*) as num_rows FROM {relation_a}
), table_b as (
    SELECT COUNT(*) as num_rows FROM {relation_b}
), row_count_diff as (
    select
        1 as id,
        table_a.num_rows - table_b.num_rows as difference
    from table_a
    cross join table_b
)
select
    row_count_diff.difference as row_count_difference,
    diff_count.num_missing as num_mismatched
from row_count_diff cross join diff_count;
'''.strip()
