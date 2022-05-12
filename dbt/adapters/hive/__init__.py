from dbt.adapters.hive.connections import HiveConnectionManager
from dbt.adapters.hive.connections import HiveCredentials
from dbt.adapters.hive.relation import HiveRelation
from dbt.adapters.hive.column import HiveColumn
from dbt.adapters.hive.impl import HiveAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import hive


Plugin = AdapterPlugin(
    adapter=HiveAdapter,
    credentials=HiveCredentials,
    include_path=hive.PACKAGE_PATH)
