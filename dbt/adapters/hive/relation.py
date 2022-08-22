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
from typing import Optional

from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.exceptions import RuntimeException

@dataclass
class HiveQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False

@dataclass
class HiveIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True

@dataclass(frozen=True, eq=False, repr=False)
class HiveRelation(BaseRelation):
    quote_policy: HiveQuotePolicy = HiveQuotePolicy()
    include_policy: HiveIncludePolicy = HiveIncludePolicy()
    quote_character: str = '`'
    is_delta: Optional[bool] = None

    @staticmethod
    def add_ephemeral_prefix(name: str):
        return f'tmp__dbt__cte__{name}'

    def __post_init__(self):
        if self.database and self.database != self.schema:
            raise RuntimeException('Cannot set database `{}` in hive!'.format(self.database))

    def render(self):
        if self.include_policy.database and self.include_policy.schema:
            raise RuntimeException(
                'Got a Hive relation with schema and database set to '
                'include, but only one can be set'
            )
        return super().render()
