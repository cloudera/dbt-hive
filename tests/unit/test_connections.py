from unittest.mock import MagicMock, patch

import pytest

from dbt.adapters.contracts.connection import Connection, ConnectionState

from dbt.adapters.hive.connections import (
    HiveConnectionManager,
    HiveCredentials,
)


JWT_PROFILE = {
    "type": "hive",
    "host": "hive.example.com",
    "port": 443,
    "schema": "dbt_test_schema",
    "auth_type": "jwt",
    "jwt": "aabbccddeeff",
    "use_http_transport": True,
    "use_ssl": True,
    "http_path": "cliservice",
}


def _make_connection(credentials: HiveCredentials) -> Connection:
    return Connection(
        type="hive",
        name="test",
        state=ConnectionState.INIT,
        transaction_open=False,
        credentials=credentials,
    )


class TestHiveJwtAuthentication:
    def test_jwt_credentials_from_profile(self):
        credentials = HiveCredentials.from_dict(JWT_PROFILE)

        assert isinstance(credentials, HiveCredentials)
        assert credentials.type == "hive"
        assert credentials.host == "hive.example.com"
        assert credentials.port == 443
        assert credentials.schema == "dbt_test_schema"
        assert credentials.auth_type == "jwt"
        assert credentials.jwt == "aabbccddeeff"
        assert credentials.use_http_transport is True
        assert credentials.use_ssl is True
        assert credentials.http_path == "cliservice"
        assert credentials.database is None

    @patch.object(HiveConnectionManager, "fetch_hive_version")
    @patch("dbt.adapters.hive.connections.impala.dbapi.connect")
    def test_jwt_open_uses_correct_connect_kwargs(self, mock_connect, _mock_fetch_version):
        mock_connect.return_value = MagicMock()
        credentials = HiveCredentials.from_dict(JWT_PROFILE)
        connection = _make_connection(credentials)

        result = HiveConnectionManager.open(connection)

        mock_connect.assert_called_once_with(
            host="hive.example.com",
            port=443,
            auth_mechanism="JWT",
            jwt="aabbccddeeff",
            use_http_transport=True,
            use_ssl=True,
            http_path="cliservice",
        )
        assert result.state == ConnectionState.OPEN
