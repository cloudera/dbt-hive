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
from collections import OrderedDict
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Union, Iterable
import agate

import dbt
import dbt.exceptions

from dbt.adapters.base import AdapterConfig
from dbt.adapters.base.impl import catch_as_completed
from dbt.adapters.sql import SQLAdapter

import dbt.adapters.hive.cloudera_tracking as tracker
from dbt.adapters.hive import HiveConnectionManager
from dbt.adapters.hive import HiveRelation
from dbt.adapters.hive import HiveColumn
from dbt.adapters.base import BaseRelation
from dbt.utils import executor

from dbt.clients import agate_helper

from dbt.events import AdapterLogger

logger = AdapterLogger("Hive")

GET_COLUMNS_IN_RELATION_MACRO_NAME = "get_columns_in_relation"
LIST_SCHEMAS_MACRO_NAME = "list_schemas"

LIST_RELATIONS_MACRO_NAME = "list_relations_without_caching"

DROP_RELATION_MACRO_NAME = "drop_relation"
FETCH_TBL_PROPERTIES_MACRO_NAME = "fetch_tbl_properties"
GET_CATALOG_MACRO_NAME = "get_catalog"

KEY_TABLE_INFORMATION = "# Detailed Table Information"
KEY_TABLE_OWNER = "Owner:"
KEY_TABLE_TYPE = "Table Type:"


@dataclass
class HiveConfig(AdapterConfig):
    file_format: str = "parquet"
    location_root: Optional[str] = None
    partition_by: Optional[Union[List[str], str]] = None
    clustered_by: Optional[Union[List[str], str]] = None
    buckets: Optional[int] = None
    options: Optional[Dict[str, str]] = None
    merge_update_columns: Optional[str] = None


class HiveAdapter(SQLAdapter):
    """Hive adaptoer
    Could not use information_schema table to retrieve metadata
    """

    Relation = HiveRelation
    Column = HiveColumn
    ConnectionManager = HiveConnectionManager
    AdapterSpecificConfigs = HiveConfig

    # @classmethod
    def date_function(cls) -> str:
        return "current_timestamp()"

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
        return f"`{identifier}`"

    def add_schema_to_cache(self, schema) -> str:
        """Cache a new schema in dbt. It will show up in `list relations`."""
        if schema is None:
            name = self.nice_connection_name()
            dbt.exceptions.raise_compiler_error(f"Attempted to cache a null schema for {name}")
        if dbt.flags.USE_CACHE:
            self.cache.add_schema(None, schema)
        # so jinja doesn't render things
        return ""

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

    def list_relations_without_caching(self, schema_relation: HiveRelation) -> List[HiveRelation]:
        """Get a list of Relation(table or view) by SQL directly
        Use different SQL statement for view/table
        """
        kwargs = {"schema": schema_relation}
        try:
            result_tables = self.execute_macro("hive__list_tables_without_caching", kwargs=kwargs)
            result_views = self.execute_macro("hive__list_views_without_caching", kwargs=kwargs)
        except dbt.exceptions.DbtRuntimeError as e:
            errmsg = getattr(e, "msg", "")
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
            is_view = (
                len(list(filter(lambda x: x["tab_name"] == row["tab_name"], result_views))) == 1
            )
            if not is_view:
                result_tables_without_view.append(row)

        relations = []
        for row in result_tables_without_view:
            relations.append(
                self.Relation.create(
                    schema=schema_relation.schema,
                    identifier=row["tab_name"],
                    type="table",
                )
            )
        for row in result_views:
            relations.append(
                self.Relation.create(
                    schema=schema_relation.schema,
                    identifier=row["tab_name"],
                    type="view",
                )
            )

        return relations

    def get_relation(self, database: str, schema: str, identifier: str) -> Optional[BaseRelation]:
        """Get a Relation for own list"""
        if not self.Relation.get_default_quote_policy().database:
            database = None

        return super().get_relation(database, schema, identifier)

    @staticmethod
    def find_table_information_separator(rows: List[dict]) -> int:
        """Returns the row number of the table information sections"""
        pos = 0
        for row in rows:
            if row["col_name"] and row["col_name"].strip() == KEY_TABLE_INFORMATION:
                break
            pos += 1
        return pos

    @staticmethod
    def parse_columns_info(raw_rows, start, end):
        # remove comments columns
        valid_rows = [
            (row)
            for row in raw_rows[start:end]
            if not row["col_name"].startswith("#") and not row["col_name"] == ""
        ]

        # avoid overridding duplicate columns by partition columns
        unique_rows = []
        for row in valid_rows:
            curr_unique_keys = list({row["col_name"]: row for row in unique_rows})
            if row["col_name"] not in curr_unique_keys:
                unique_rows.append(row)
        return unique_rows

    def parse_describe_formatted(
        self, relation: Relation, raw_rows: List[agate.Row]
    ) -> List[HiveColumn]:
        # Convert the Row to a dict
        dict_rows = [dict(zip(row._keys, row._values)) for row in raw_rows]
        # Find the separator between columns and partitions information
        # by the DESCRIBE EXTENDED {{relation}} statement
        partition_separator_pos = self.find_partition_information_separator(dict_rows)

        # Find the separator between the rows and the metadata provided
        # by the DESCRIBE EXTENDED {{relation}} statement
        table_separator_pos = self.find_table_information_separator(dict_rows)

        column_separator_pos = (
            partition_separator_pos if partition_separator_pos > 0 else table_separator_pos
        )
        logger.debug(
            f"relation={relation}, "
            f"partition_separator_pos = {partition_separator_pos}, "
            f"table_separator_pos = {table_separator_pos}, "
            f"column_separator_pos = {column_separator_pos}"
        )

        # fetch columns info
        rows = self.parse_columns_info(raw_rows, 0, table_separator_pos)

        metadata = {
            col["col_name"].split(":")[0].strip(): col["data_type"].strip()
            for col in raw_rows[table_separator_pos + 1 :]
            if col["col_name"] and not col["col_name"].startswith("#") and col["data_type"]
        }

        # raw_table_stats = metadata.get(KEY_TABLE_STATISTICS)
        # table_stats = HiveColumn.convert_table_stats(raw_table_stats)

        # strip white spaces
        new_metadata = {}
        for k in metadata:
            new_metadata[k.strip()] = metadata[k].strip() if metadata[k] else ""
        metadata = new_metadata

        return [
            HiveColumn(
                table_database=None,
                table_schema=relation.schema,
                table_name=relation.name,
                table_type=relation.type,
                table_owner=str(metadata.get(KEY_TABLE_OWNER)),
                table_stats=None,
                column=column["col_name"],
                column_index=idx,
                dtype=column["data_type"],
            )
            for idx, column in enumerate(rows)
        ]

    @staticmethod
    def find_partition_information_separator(rows: List[dict]) -> int:
        pos = 0
        ps_keys = [
            "# Partition Information",  # non-iceberg
            "# Partition Transform Information",  # iceberg
        ]
        for row in rows:
            if row["col_name"] and row["col_name"].startswith(tuple(ps_keys)):
                break
            pos += 1
        result = 0 if (pos == len(rows)) else pos
        return result

    @staticmethod
    def find_table_information_separator(rows: List[dict]) -> int:
        """Find the position of table defailt information"""
        pos = 0
        for row in rows:
            if row["col_name"] == "# Detailed Table Information":
                break
            pos += 1
        return pos

    def get_columns_in_relation(self, relation: Relation) -> List[HiveColumn]:
        """Get columns from a Relation
        parent method is used to call DESCRIBE <tablename> statement
        dtype is used for correct quote
        """
        try:
            rows: List[agate.Row] = super().get_columns_in_relation(relation)
            columns = self.parse_describe_formatted(relation, rows)
        except dbt.exceptions.DbtRuntimeError as e:
            # impala would throw error when table doesn't exist
            errmsg = getattr(e, "msg", "")
            if (
                "Table or view not found" in errmsg
                or "NoSuchTableException" in errmsg
                or "Could not resolve path" in errmsg
                or "Table not found" in errmsg
            ):
                return []
            else:
                raise e

        return columns

    def _get_columns_for_catalog(self, relation: HiveRelation) -> Iterable[Dict[str, Any]]:
        """Get columns for catalog. Used by get_one_catalog"""
        # columns = self.parse_columns_from_information(relation)
        columns = self.get_columns_in_relation(relation)

        for column in columns:
            # convert HiveColumns into catalog dicts
            as_dict = column.to_column_dict()
            as_dict["column_name"] = as_dict.pop("column", None)
            as_dict["column_type"] = as_dict.pop("dtype")
            as_dict["table_database"] = None
            yield as_dict

    def get_properties(self, relation: Relation) -> Dict[str, str]:
        """ """
        properties = self.execute_macro(
            FETCH_TBL_PROPERTIES_MACRO_NAME, kwargs={"relation": relation}
        )
        return dict(properties)

    def get_catalog(self, manifest):
        """Return a catalogs that contains information of all schemas"""
        schema_map = self._get_catalog_schemas(manifest)
        if len(schema_map) > 1:
            dbt.exceptions.raise_compiler_error(
                f"Expected only one database in get_catalog, found " f"{list(schema_map)}"
            )

        # run heavy job in other threads
        with executor(self.config) as tpe:
            futures: List[Future[agate.Table]] = []
            for info, schemas in schema_map.items():
                for schema in schemas:
                    futures.append(
                        tpe.submit_connected(
                            self,
                            schema,
                            self._get_one_catalog,
                            info,
                            [schema],
                            manifest,
                        )
                    )
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
        self,
        information_schema,
        schemas,
        manifest,
    ) -> agate.Table:
        """Get ONE catalog. Used by get_catalog

        manifest is used to run the method in other context's
        threadself.get_columns_in_relation
        """
        if len(schemas) != 1:
            dbt.exceptions.raise_compiler_error(
                f"Expected only one schema in Hive _get_one_catalog, found " f"{schemas}"
            )

        database = information_schema.database
        schema = list(schemas)[0]

        schema_relation = self.Relation.create(
            database=database,
            schema=schema,
            identifier="",
            quote_policy=self.config.quoting,
        ).without_identifier()

        columns: List[Dict[str, Any]] = []
        for relation in self.list_relations(database, schema):
            logger.debug(f"Getting table schema for relation {relation}")
            columns.extend(self._get_columns_for_catalog(relation))

        if len(columns) > 0:
            text_types = agate_helper.build_type_tester(["table_owner", "table_database"])
        else:
            text_types = []

        return agate.Table.from_object(
            columns,
            column_types=text_types,
        )

    def check_schema_exists(self, database, schema):
        results = self.execute_macro(LIST_SCHEMAS_MACRO_NAME, kwargs={"database": database})

        exists = True if schema in [row[0] for row in results] else False
        return exists

    def debug_query(self) -> None:
        self.execute("select 1 as id")

        try:
            username = self.config.credentials.username
            sql_query = "show grant user `" + username + "` on server"
            _, table = self.execute(sql_query, True, True)
            permissions_object = []
            json_funcs = [c.jsonify for c in table.column_types]

            for row in table.rows:
                values = tuple(json_funcs[i](d) for i, d in enumerate(row))
                permissions_object.append(OrderedDict(zip(row.keys(), values)))

            permissions_json = permissions_object

            payload = {
                "event_type": tracker.TrackingEventType.DEBUG,
                "permissions": permissions_json,
            }
            tracker.track_usage(payload)
        except Exception as ex:
            logger.debug(f"Failed to fetch permissions for user: {username}. Exception: {ex}")
            self.connections.get_thread_connection().handle.close()

        self.connections.get_thread_connection().handle.close()

    ###
    # Methods about grants
    ###
    def standardize_grants_dict(self, grants_table: agate.Table) -> dict:
        """Translate the result of `show grants` (or equivalent) to match the
        grants which a user would configure in their project.
        Ideally, the SQL to show grants should also be filtering:
        filter OUT any grants TO the current user/role (e.g. OWNERSHIP).
        If that's not possible in SQL, it can be done in this method instead.
        :param grants_table: An agate table containing the query result of
            the SQL returned by get_show_grant_sql
        :return: A standardized dictionary matching the `grants` config
        :rtype: dict
        """
        unsupported_privileges = ["INDEX", "READ", "WRITE"]

        grants_dict: Dict[str, List[str]] = {}
        for row in grants_table:
            grantee = row["grantor"]
            privilege = row["privilege"]

            # skip unsupported privileges
            if privilege in unsupported_privileges:
                continue

            if privilege in grants_dict.keys():
                grants_dict[privilege].append(grantee)
            else:
                grants_dict.update({privilege: [grantee]})
        return grants_dict

    def get_rows_different_sql(
        self,
        relation_a: BaseRelation,
        relation_b: BaseRelation,
        column_names: Optional[List[str]] = None,
        except_operator: str = "EXCEPT",
    ) -> str:
        """Generate SQL for a query that returns a single row with a two
        columns: the number of rows that are different between the two
        relations and the number of mismatched rows.
        """

        # This method only really exists for test reasons.
        names: List[str]
        if column_names is None:
            columns = self.get_columns_in_relation(relation_a)
            names = sorted(self.quote(c.name) for c in columns)
        else:
            names = sorted(self.quote(n) for n in column_names)
        columns_csv = ", ".join(names)

        sql = COLUMNS_EQUAL_SQL.format(
            columns=columns_csv,
            relation_a=str(relation_a),
            relation_b=str(relation_b),
            except_op=except_operator,
        )

        return sql

    def valid_incremental_strategies(self):
        """The set of standard builtin strategies which this adapter supports out-of-the-box.
        Not used to validate custom strategies defined by end users.
        """
        return ["append", "insert_overwrite", "merge"]


COLUMNS_EQUAL_SQL = """
with diff_missing as (
    (SELECT {columns} FROM {relation_a} {except_op}
             SELECT {columns} FROM {relation_b})
        UNION ALL
    (SELECT {columns} FROM {relation_b} {except_op}
             SELECT {columns} FROM {relation_a})
),
diff_count as (
    SELECT
        1 as id,
        COUNT(*) as num_missing FROM diff_missing as a
), table_a as (
    SELECT COUNT(*) as num_rows FROM {relation_a}
), table_b as (
    SELECT COUNT(*) as num_rows FROM {relation_b}
), row_count_diff as (
    select
        1 as id,
        table_a.num_rows - table_b.num_rows as difference
    from table_a, table_b
)
select
    row_count_diff.difference as row_count_difference,
    diff_count.num_missing as num_mismatched
from row_count_diff
join diff_count using (id)
""".strip()
