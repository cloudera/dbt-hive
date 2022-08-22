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
from contextlib import contextmanager

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import ConnectionState
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import DECIMALS
from dbt.adapters.hive import __version__

from dbt.contracts.connection import Connection, AdapterResponse

from dbt.events.functions import fire_event
from dbt.events.types import ConnectionUsed, SQLQuery, SQLQueryStatus

from datetime import datetime
import sqlparams

from hologram.helpers import StrEnum
from dataclasses import dataclass, field
from typing import Any, Optional, Dict, Tuple
import base64
import time

import impala.dbapi

NUMBERS = DECIMALS + (int, float)

DEFAULT_HIVE_PORT = 10000

@dataclass
class HiveCredentials(Credentials):
    # Add credentials members here, like:
    host: str = 'localhost'
    schema: str = None
    port: Optional[int] = DEFAULT_HIVE_PORT
    database: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    auth_type: Optional[str] = None
    use_ssl: Optional[bool] = True
    use_http_transport: Optional[bool] = True
    http_path: Optional[str] = None

    @classmethod
    def __pre_deserialize__(cls, data):
        data = super().__pre_deserialize__(data)
        # ignore database setting
        data['database'] = None
        return data

    def __post_init__(self):
        # hive classifies database and schema as the same thing
        if (
            self.database is not None and
            self.database != self.schema
        ):
            raise dbt.exceptions.RuntimeException(
                f'    schema: {self.schema} \n'
                f'    database: {self.database} \n'
                f'On Hive, database must be omitted or have the same value as'
                f' schema.'
            )
        self.database = None


    @property
    def type(self):
        return 'hive'

    def _connection_keys(self):
        return ('host','schema','user')


class HiveConnectionWrapper(object):
    """Wrap a Hive connection in a way that no-ops transactions"""
    # https://forums.databricks.com/questions/2157/in-apache-hive-sql-can-we-roll-back-the-transacti.html  # noqa

    def __init__(self, handle):
        self.handle = handle
        self._cursor = None

    def cursor(self):
        self._cursor = self.handle.cursor()
        return self

    def cancel(self):
        if self._cursor:
            # Handle bad response in the pyhive lib when
            # the connection is cancelled
            try:
                self._cursor.cancel()
            except EnvironmentError as exc:
                logger.debug(
                    "Exception while cancelling query: {}".format(exc)
                )

    def close(self):
        if self._cursor:
            # Handle bad response in the pyhive lib when
            # the connection is cancelled
            try:
                self._cursor.close()
            except EnvironmentError as exc:
                logger.debug(
                    "Exception while closing cursor: {}".format(exc)
                )

    def rollback(self, *args, **kwargs):
        logger.debug("NotImplemented: rollback")

    def fetchall(self):
        return self._cursor.fetchall()

    def fetchone(self):
        return self._cursor.fetchone()

    def execute(self, sql, bindings=None, configuration={}):
        if sql.strip().endswith(";"):
            sql = sql.strip()[:-1]

        if bindings is not None:
            bindings = [self._fix_binding(binding) for binding in bindings]

        result = self._cursor.execute(sql,bindings,configuration)
        return result


    @classmethod
    def _fix_binding(cls, value):
        """Convert complex datatypes to primitives that can be loaded by
           the Hive driver"""
        if value is None:
            return 'NULL'
        elif isinstance(value, NUMBERS):
            return float(value)
        elif isinstance(value, datetime):
            return value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        elif isinstance(value, str):
            return "'{}'".format(value.replace("'", "''"))
        else:
            return value

    @property
    def description(self):
        return self._cursor.description


class HiveConnectionManager(SQLConnectionManager):
    TYPE = 'hive'

    @classmethod
    def open(cls, connection):
        if connection.state == ConnectionState.OPEN:
           logger.debug('Connection is already open, skipping open.')
           return connection

        credentials = connection.credentials

        # add configuration to yaml
        if (not credentials.auth_type):
           hive_conn = impala.dbapi.connect(
                         host=credentials.host, 
                         port=credentials.port
                   )
        elif (credentials.auth_type.upper() == 'LDAP'):
           hive_conn = impala.dbapi.connect(
                         host=credentials.host,
                         port=credentials.port,
                         auth_mechanism='LDAP',
                         use_http_transport=credentials.use_http_transport,
                         user=credentials.user,
                         password=credentials.password,
                         use_ssl=credentials.use_ssl,
                         http_path=credentials.http_path
                   )

        connection.state = ConnectionState.OPEN
        connection.handle = HiveConnectionWrapper(hive_conn)
        return connection

    @contextmanager
    def exception_handler(self, sql: str):
        try:
            yield
        except Exception as exc:
            logger.debug("Error while running:\n{}".format(sql))
            logger.debug(exc)
            if len(exc.args) == 0:
                raise
            raise dbt.exceptions.RuntimeException(str(exc))


    def cancel(self, connection):
        connection.handle.cancel()

    def close(self, connection):
        if connection.handle:
            connection.handle.close()
            connection.state = ConnectionState.CLOSED

    @classmethod
    def get_response(cls, cursor):
        message = 'OK'
        return AdapterResponse(
            _message=message
        )

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False
    ) -> Tuple[Connection, Any]:
        
        connection = self.get_thread_connection()
        if auto_begin and connection.transaction_open is False:
            self.begin()
        fire_event(ConnectionUsed(conn_type=self.TYPE, conn_name=connection.name))

        with self.exception_handler(sql):
            if abridge_sql_log:
                log_sql = '{}...'.format(sql[:512])
            else:
                log_sql = sql

            fire_event(SQLQuery(conn_name=connection.name, sql=log_sql))
            pre = time.time()

            cursor = connection.handle.cursor()

            # paramstlye parameter is needed for the datetime object to be correctly qouted when
            # running substitution query from impyla. this fix also depends on a patch for impyla:
            # https://github.com/cloudera/impyla/pull/486
            configuration = {}
            configuration['paramstyle'] = 'format'
            cursor.execute(sql, bindings, configuration)

            fire_event(
                SQLQueryStatus(
                    status=str(self.get_response(cursor)),
                    elapsed=round((time.time() - pre), 2)
                )
            )

            return connection, cursor

    # No transactions on Hive....
    def add_begin_query(self, *args, **kwargs):
        logger.debug("NotImplemented: add_begin_query")

    def add_commit_query(self, *args, **kwargs):
        logger.debug("NotImplemented: add_commit_query")

    def commit(self, *args, **kwargs):
        logger.debug("NotImplemented: commit")

    def rollback(self, *args, **kwargs):
        logger.debug("NotImplemented: rollback")

    @classmethod
    def validate_creds(cls, creds, required):
        method = creds.method

        for key in required:
            if not hasattr(creds, key):
                raise dbt.exceptions.DbtProfileError(
                    "The config '{}' is required when using the {} method"
                    " to connect to Hive".format(key, method))

        else:
            raise exc


