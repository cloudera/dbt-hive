from contextlib import contextmanager

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import ConnectionState
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import DECIMALS
from dbt.adapters.hive import __version__

from datetime import datetime
import sqlparams

from hologram.helpers import StrEnum
from dataclasses import dataclass, field
from typing import Any, Optional, Dict
import base64
import time

from pyhive import hive

NUMBERS = DECIMALS + (int, float)



@dataclass
class HiveCredentials(Credentials):
    # Add credentials members here, like:
    host: str = 'localhost'
    schema: str = None
    database: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    auth: Optional[str] = None
    session_properties: Dict[str, Any] = field(default_factory=dict)

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

    def execute(self, sql, bindings=None):
        if sql.strip().endswith(";"):
            sql = sql.strip()[:-1]

        if bindings is not None:
            bindings = [self._fix_binding(binding) for binding in bindings]

        result = self._cursor.execute(sql,bindings)
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
        configuration = {k: str(v) for k,v in credentials.session_properties.copy().items()}

        # add configuration to yaml
        hive_conn = hive.Connection(
            host=credentials.host,
            # database=credentials.schema,  # in hive schema = database
            username=credentials.user,
            password=credentials.password,
            auth=credentials.auth,

            # need to parameterize those
            # configuration = {
            #     'hive.groupby.orderby.position.alias':'true',
            #     'hive.vectorized.execution.enabled':'false',
            #     'hive.vectorized.execution.reduce.enabled':'false',
            #     'hive.exec.dynamic.partition':'true',
            #     'hive.exec.dynamic.partition.mode':'nonstrict'
            # }
            configuration = configuration
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
        return 'OK'

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


