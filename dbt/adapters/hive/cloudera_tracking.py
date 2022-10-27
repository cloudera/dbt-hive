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

import dbt.version
import json
import os
import platform
import requests
import sys
import time
import hashlib
import threading
from dbt.tracking import active_user

from dbt.adapters.base import Credentials
from dbt.events import AdapterLogger
from decouple import config

# global logger
logger = AdapterLogger("Tracker")

# global switch to turn on/off the usage tracking
usage_tracking: bool = True

# Json object to store OS and platform related information
platform_info = {}

# Json object to store unique ID
unique_ids = {}

# Json object to store dbt profile(profile.yml) related information
profile_info = {}

# Json object to store dbt deployment environment variables
dbt_deployment_env_info = {}

def populate_platform_info(cred: Credentials, ver):
    """
    populate platform info to be passed on for tracking
    @param cred DBT cred object, representing the dbt profile
    @param ver DBT adapter version
    """
    # Python version e.g: 2.6.5
    platform_info["python_version"] = sys.version.split()[0]
    # Underlying system e.g. : Linux, Darwin(Mac), Windows
    platform_info["system"] = platform.system()
    # Architecture e.g. x86_64 ,arm, AMD64
    platform_info["machine"] = platform.machine()
    # Full platform info e.g.
    # Linux-2.6.32-32-server-x86_64-with-Ubuntu-10.04-lucid,
    # Windows-2008ServerR2-6.1.7601-SP1
    platform_info["platform"] = platform.platform()
    # dbt core version
    platform_info[
        "dbt_version"
    ] = dbt.version.get_installed_version().to_version_string(skip_matcher=True)
    # dbt adapter info e.g. impala-1.2.0
    platform_info["dbt_adapter"] = f"{cred.type}-{ver.version}"

def populate_dbt_deployment_env_info():
    """
    populate dbt deployment environment variables if available to be passed on for tracking
    """
    default_value = ""  # if environment variables doesn't exist add empty string as default
    dbt_deployment_env_info["dbt_deployment_env"] = os.environ.get('DBT_DEPLOYMENT_ENV', default_value)

def populate_unique_ids(cred: Credentials):
    host = str(cred.host).encode()
    user = str(cred.username).encode()
    timestamp = str(time.time()).encode()

    # dbt invocation id
    if active_user:
       unique_ids["id"] = active_user.invocation_id
    else:
       unique_ids["id"] = "N/A"
    # hashed host name
    unique_ids["unique_host_hash"] = hashlib.md5(host).hexdigest()
    # hashed username
    unique_ids["unique_user_hash"] = hashlib.md5(user).hexdigest()
    # hashed session
    unique_ids["unique_session_hash"] = hashlib.md5(host + user + timestamp).hexdigest()


def generate_profile_info(self):
    if not profile_info:
        # name of dbt project in profiles
        profile_info["project_name"] = self.profile.profile_name
        # dbt target in profiles
        profile_info["target_name"] = self.profile.target_name
        # number of threads in profiles
        profile_info["no_of_threads"] = self.profile.threads


def _merge_keys(source_dict, dest_dict):
    for key, value in source_dict.items():
        dest_dict[key] = value
    return dest_dict


def _get_sql_type(sql):
    if not sql:
        return ""

    words = sql.split("*/")

    if len(words) > 1:
        sql_words = words[1].strip().split()
    else:
        sql_words = words[0].strip().split()

    sql_type = " ".join(sql_words[: min(2, len(sql_words))]).lower()

    return sql_type


def fix_tracking_payload(given_payload):
    """
    The payload for an event
    @param given_payload: Payload sent from events
    @return desired_payload: Payload in desired schema
    """
    desired_payload = {}
    # merge valid keys from source to desired payload first
    desired_payload = _merge_keys(given_payload, desired_payload)

    # handle sql redaction - convert to sql type from full sql statement
    if "sql" in desired_payload:
        desired_payload["sql_type"] = _get_sql_type(desired_payload["sql"])
        del desired_payload["sql"]

    desired_keys = [
        "auth",
        "connection_state",
        "elapsed_time",
        "incremental_strategy",
        "model_name",
        "model_type",
        "permissions",
        "profile_name",
        "sql_type",
    ]

    for key in desired_keys:
        if key not in desired_payload:
            # indicate that the key doesn't have valid data for the event
            desired_payload[key] = "N/A"

    return desired_payload


def track_usage(tracking_payload):
    """
    usage tracking code - Cloudera specific
    @param tracking_payload:
    @param tracking_payload - list of key value pair of tracking data.
    Example:
            payload = {}
            payload["id"] = "dbt_impala_open"
            payload["unique_hash"] = hashlib.md5(credentials.host.encode()).hexdigest()
            payload["auth"] = auth_type
            payload["connection_state"] = connection.state
    """

    global usage_tracking

    logger.debug(
        f"Usage tracking flag {usage_tracking}."
        f"To turn on/off use usage_tracking flag in profiles.yml"
    )

    # if usage_tracking is disabled, quit
    if not usage_tracking:
        logger.debug(f"Skipping Event {tracking_payload}")
        return

    # fix the schema of tracking payload to be common for all events
    tracking_payload = fix_tracking_payload(tracking_payload)
    # inject other static payload to tracking_payload
    tracking_payload = _merge_keys(unique_ids, tracking_payload)
    tracking_payload = _merge_keys(platform_info, tracking_payload)
    tracking_payload = _merge_keys(dbt_deployment_env_info, tracking_payload)
    tracking_payload = _merge_keys(profile_info, tracking_payload)

    # form the tracking data
    tracking_data = {"data": json.dumps(tracking_payload)}

    # inner function which actually calls the endpoint
    def _tracking_func(data):
        global usage_tracking

        try:
            SNOWPLOW_ENDPOINT = config("SNOWPLOW_ENDPOINT")
            SNOWPLOW_TIMEOUT = int(config("SNOWPLOW_TIMEOUT"))  # 10 seconds
            SNOWPLOW_API_KEY = config("SNOWPLOW_API_KEY")
            SNOWPLOW_ENV = config("SNOWPLOW_ENV")
        except Exception as err:
            logger.debug(f"Error reading tracking config. {err}")
            logger.debug("Disabling usage tracking due to error.")
            usage_tracking = False
            return

        # prod creds
        headers = {
            "x-api-key": SNOWPLOW_API_KEY,
            "x-datacoral-environment": SNOWPLOW_ENV,
            "x-datacoral-passthrough": "true",
        }

        data = json.dumps([data])

        res = None

        try:
            logger.debug(f"Sending Event {data}")
            res = requests.post(
                SNOWPLOW_ENDPOINT,
                data=data,
                headers=headers,
                timeout=SNOWPLOW_TIMEOUT
            )
        except Exception as err:
            logger.debug(f"Usage tracking error. {err}")
            logger.debug("Disabling usage tracking due to error.")
            usage_tracking = False

        return res

    # call the tracking function in a Thread
    the_track_thread = threading.Thread(
        target=_tracking_func, kwargs={"data": tracking_data}
    )
    the_track_thread.start()
