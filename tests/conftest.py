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
        'type': 'hive',
        'threads': 4,
        'schema': 'dbt_hive_adapter_test',
        'host':  os.getenv('HIVE_HOST'),
        'http_path': os.getenv('HIVE_HTTP_PATH'),
        'port': 443,
        'auth_type': 'LDAP',
        'use_http_transport': True,
        'use_ssl': True,
        'username': os.getenv('HIVE_USER'),
        'password': os.getenv('HIVE_PASSWORD'),
        'threads': 4,
    }
# 
from _pytest.assertion import truncate
truncate.DEFAULT_MAX_LINES = 9999
truncate.DEFAULT_MAX_CHARS = 9999
