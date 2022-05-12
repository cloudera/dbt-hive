import pytest
import os

# Import the standard functional fixtures as a plugin
# Note: fixtures with session scope need to be local
pytest_plugins = ["dbt.tests.fixtures.project"]

# The profile dictionary, used to write out profiles.yml
# dbt will supply a unique schema per test, so we do not specify 'schema' here
@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        # 'type': 'trino',
        # 'database': 'hive',
        # 'port': 443,
        # 'threads': 1,
        # 'auth': 'LDAP',
        # 'method': 'ldap',
        # 'host': 'trino.d4b.jp',
        # 'user': os.getenv('TRINO_USER'),
        # 'password': os.getenv('TRINO_PASSWORD'),
        'threads': 1,
        'type': 'hive',
        'host': '172.31.0.16',
        'auth': 'LDAP',
        'user': os.getenv('HIVE_USER'),
        'password': os.getenv('HIVE_PASSWORD'),
    }

# 
from _pytest.assertion import truncate
truncate.DEFAULT_MAX_LINES = 9999
truncate.DEFAULT_MAX_CHARS = 9999
