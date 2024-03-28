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
import time
import json
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, Tuple

import dbt.exceptions
import impala.dbapi
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import (
    AdapterResponse,
    AdapterRequiredConfig,
    Connection,
    ConnectionState,
)
from dbt.events import AdapterLogger
from dbt.events.functions import fire_event
from dbt.events.types import ConnectionUsed, SQLQuery, SQLQueryStatus
from dbt.utils import DECIMALS

import json
import time

import impala.dbapi
from impala.error import HttpError
from impala.error import HiveServer2Error

import dbt.adapters.hive.__version__ as ver
import dbt.adapters.hive.cloudera_tracking as tracker

logger = AdapterLogger("Hive")

NUMBERS = DECIMALS + (int, float)

DEFAULT_HIVE_PORT = 10000


@dataclass
class HiveCredentials(Credentials):
    # Add credentials members here, like:
    host: str = "localhost"
    schema: str = None
    port: Optional[int] = DEFAULT_HIVE_PORT
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    auth_type: Optional[str] = None
    use_ssl: Optional[bool] = True
    ca_cert: Optional[str] = None
    use_http_transport: Optional[bool] = True
    http_path: Optional[str] = None
    kerberos_service_name: Optional[str] = None
    usage_tracking: Optional[bool] = True  # usage tracking is enabled by default

    _ALIASES = {"pass": "password", "user": "username"}

    @classmethod
    def __pre_deserialize__(cls, data):
        data = super().__pre_deserialize__(data)
        # ignore database setting
        data["database"] = None
        return data

    def __post_init__(self):
        # hive classifies database and schema as the same thing
        if self.database is not None and self.database != self.schema:
            raise dbt.exceptions.DbtRuntimeError(
                f"    schema: {self.schema} \n"
                f"    database: {self.database} \n"
                f"On Hive, database must be omitted or have the same value as"
                f" schema."
            )
        self.database = None
        # set the usage tracking flag
        tracker.usage_tracking = self.usage_tracking
        # get platform information for tracking
        tracker.populate_platform_info(self, ver)
        # get dbt deployment information for tracking
        tracker.populate_dbt_deployment_env_info()
        # generate unique ids for tracking
        tracker.populate_unique_ids(self)

    @property
    def type(self):
        return "hive"

    @property
    def unique_field(self) -> str:
        return f"{self.host}_{self.schema}_{self.username}"

    def _connection_keys(self):
        return "host", "schema", "user"


class HiveConnectionWrapper:
    """Wrap a Hive connection in a way that no-ops transactions"""

    # https://forums.databricks.com/questions/2157/in-apache-hive-sql-can-we-roll-back-the-transacti.html  # noqa

    def __init__(self, handle):
        self.handle = handle
        self._cursor = None

    def cursor(self):
        if not self._cursor:
            self._cursor = self.handle.cursor()
        return self

    def cancel(self):
        if self._cursor:
            # Handle bad response in the pyhive lib when
            # the connection is cancelled
            try:
                self._cursor.cancel()
            except OSError as exc:
                logger.debug(f"Exception while cancelling query: {exc}")

    def close(self):
        if self._cursor:
            # Handle bad response in the pyhive lib when
            # the connection is cancelled
            try:
                self._cursor.close()
            except OSError as exc:
                logger.debug(f"Exception while closing cursor: {exc}")

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

        result = self._cursor.execute(sql, bindings, configuration)
        return result

    @classmethod
    def _fix_binding(cls, value):
        """Convert complex datatypes to primitives that can be loaded by
        the Hive driver"""
        if value is None:
            return "NULL"
        elif isinstance(value, NUMBERS):
            return float(value)
        elif isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        else:
            return value

    @property
    def description(self):
        return self._cursor.description


class HiveConnectionManager(SQLConnectionManager):
    TYPE = "hive"

    hive_version = None

    def __init__(self, profile: AdapterRequiredConfig):
        super().__init__(profile)
        # generate profile related object for instrumentation.
        tracker.generate_profile_info(self)

    @classmethod
    def open(cls, connection):
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = connection.credentials
        connection_ex = None

        auth_type = "insecure"
        hive_conn = None
        try:
            connection_start_time = time.time()
            # add configuration to yaml
            if not credentials.auth_type:
                hive_conn = impala.dbapi.connect(
                    host=credentials.host, port=credentials.port, auth_mechanism="PLAIN"
                )
            elif credentials.auth_type.upper() == "LDAP":
                auth_type = "ldap"
                hive_conn = impala.dbapi.connect(
                    host=credentials.host,
                    port=credentials.port,
                    auth_mechanism="LDAP",
                    use_http_transport=credentials.use_http_transport,
                    user=credentials.username,
                    password=credentials.password,
                    use_ssl=credentials.use_ssl,
                    http_path=credentials.http_path,
                )
            elif (
                credentials.auth_type.upper() == "GSSAPI"
                or credentials.auth_type.upper() == "KERBEROS"
            ):  # kerberos based connection
                auth_type = "kerberos"
                hive_conn = impala.dbapi.connect(
                    host=credentials.host,
                    port=credentials.port,
                    auth_mechanism="GSSAPI",
                    kerberos_service_name=credentials.kerberos_service_name,
                    use_http_transport=credentials.use_http_transport,
                    use_ssl=credentials.use_ssl,
                    ca_cert=credentials.ca_cert,
                )
            else:
                raise dbt.exceptions.DbtProfileError(
                    f"Invalid auth_type {credentials.auth_type} provided"
                )
            connection_end_time = time.time()

            connection.state = ConnectionState.OPEN
            connection.handle = HiveConnectionWrapper(hive_conn)
            connection.handle.cursor()

            HiveConnectionManager.fetch_hive_version(connection.handle)
        except Exception as exc:
            logger.error(f"Connection error: {exc}")
            connection_ex = exc
            connection.state = ConnectionState.FAIL
            connection.handle = None
            connection_end_time = time.time()

        # track usage
        payload = {
            "event_type": tracker.TrackingEventType.OPEN,
            "auth": auth_type,
            "connection_state": connection.state,
            "elapsed_time": f"{connection_end_time - connection_start_time:.2f}",
        }

        if connection.state == ConnectionState.FAIL:
            payload["connection_exception"] = f"{connection_ex}"
            tracker.track_usage(payload)
            raise connection_ex
        else:
            tracker.track_usage(payload)

        return connection

    @contextmanager
    def exception_handler(self, sql: str):
        try:
            yield
        except HttpError as httpError:
            logger.debug(f"Authorization error: {httpError}")
            raise dbt.exceptions.DbtRuntimeError(
                "HTTP Authorization error: " + str(httpError) + ", please check your credentials"
            )
        except HiveServer2Error as hiveError:
            logger.debug(f"Server connection error: {hiveError}")
            raise dbt.exceptions.DbtRuntimeError(
                "Unable to establish connection to Hive server: " + str(hiveError)
            )
        except Exception as exc:
            logger.debug(f"Error while running:\n{sql}")
            logger.debug(exc)
            if len(exc.args) == 0:
                raise
            raise dbt.exceptions.DbtRuntimeError(str(exc))

    def cancel(self, connection):
        connection.handle.cancel()

    @classmethod
    def fetch_hive_version(cls, connection):
        if HiveConnectionManager.hive_version:
            return HiveConnectionManager.hive_version

        try:
            sql = "select version()"
            cursor = connection.cursor()
            cursor.execute(sql)

            res = cursor.fetchall()

            HiveConnectionManager.hive_version = res[0][0].split(".")[0].strip()

            tracker.populate_warehouse_info(
                {"version": HiveConnectionManager.hive_version, "build": res[0][0]}
            )
        except Exception as ex:
            # we couldn't get the hive warehouse version
            logger.debug(f"Cannot get hive version. Error: {ex}")
            HiveConnectionManager.impala_version = "NA"

            tracker.populate_warehouse_info(
                {"version": HiveConnectionManager.hive_version, "build": "NA"}
            )

        logger.debug(f"HIVE VERSION {'HiveConnectionManager.hive_version'}")

    @classmethod
    def close(cls, connection):
        try:
            # if the connection is in closed or init, there's nothing to do
            if connection.state in {ConnectionState.CLOSED, ConnectionState.INIT}:
                return connection

            connection_close_start_time = time.time()
            connection = super().close(connection)
            connection_close_end_time = time.time()

            payload = {
                "event_type": tracker.TrackingEventType.CLOSE,
                "connection_state": ConnectionState.CLOSED,
                "elapsed_time": "{:.2f}".format(
                    connection_close_end_time - connection_close_start_time
                ),
            }

            tracker.track_usage(payload)

            return connection
        except Exception as err:
            logger.debug(f"Error closing connection {err}")

    @classmethod
    def get_response(cls, cursor):
        message = "OK"
        return AdapterResponse(_message=message)

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
    ) -> Tuple[Connection, Any]:
        connection = self.get_thread_connection()
        if auto_begin and connection.transaction_open is False:
            self.begin()
        fire_event(ConnectionUsed(conn_type=self.TYPE, conn_name=connection.name))

        additional_info = {}
        if self.query_header:
            try:
                additional_info = json.loads(self.query_header.comment.query_comment.strip())
            except Exception as ex:  # silently ignore error for parsing
                additional_info = {}
                logger.debug(f"Unable to get query header {ex}")

        strip_sql = sql.split("*/")
        if len(strip_sql) > 1:
            strip_sql = strip_sql[1]
        else:
            strip_sql = strip_sql[0]

        if strip_sql.strip().lower().startswith("set"):
            sql = strip_sql

        with self.exception_handler(sql):
            if abridge_sql_log:
                log_sql = f"{sql[:512]}..."
            else:
                log_sql = sql

            # track usage
            payload = {
                "event_type": tracker.TrackingEventType.START_QUERY,
                "sql": log_sql,
                "profile_name": self.profile.profile_name,
            }

            for key, value in additional_info.items():
                if key == "node_id":
                    payload["model_name"] = value
                else:
                    payload[key] = value

            tracker.track_usage(payload)

            fire_event(SQLQuery(conn_name=connection.name, sql=log_sql))
            pre = time.time()

            cursor = connection.handle.cursor()

            # paramstyle parameter is needed for the datetime object to be correctly quoted when
            # running substitution query from impyla. this fix also depends on a patch for impyla:
            # https://github.com/cloudera/impyla/pull/486

            if bindings:
                # to avoid None as being treated as string, convert it to empty string
                bindings = list(
                    map(lambda x: x if x else x if x != None and len(str(x)) > 0 else "", bindings)
                )

            query_exception = None
            try:
                configuration = {"paramstyle": "format"}
                cursor.execute(sql, bindings, configuration)
                query_status = str(self.get_response(cursor))
            except Exception as ex:
                query_status = str(ex)
                query_exception = ex

            elapsed_time = time.time() - pre

            payload = {
                "event_type": tracker.TrackingEventType.END_QUERY,
                "sql": log_sql,
                "elapsed_time": f"{elapsed_time:.2f}",
                "status": query_status,
                "profile_name": self.profile.profile_name,
            }

            tracker.track_usage(payload)

            # re-raise query exception so that it propogates to dbt
            if query_exception:
                raise query_exception

            fire_event(
                SQLQueryStatus(
                    status=str(self.get_response(cursor)),
                    elapsed=round(elapsed_time, 2),
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
                    " to connect to Hive".format(key, method)
                )
