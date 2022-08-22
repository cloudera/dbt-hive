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
