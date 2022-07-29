#!/usr/bin/env python
from setuptools import find_namespace_packages, setup

package_name = "dbt-hive"
# make sure this always matches dbt/adapters/hive/__version__.py
package_version = "1.1.0b3"
description = """The experient hive adapter plugin for dbt"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author='Nguyen Huu Bach',
    author_email='bachng@gmail.com',
    url='https://github.com/bachng2017/dbt-hive',
    packages=find_namespace_packages(include=['dbt', 'dbt.*']),
    package_data={
        "dbt": [
            "include/hive/dbt_project.yml",
            "include/hive/sample_profiles.yml",
            "include/hive/macros/*.sql",
            "include/hive/macros/*/*.sql",
            "include/hive/macros/*/*/*.sql",
        ]
    },
    install_requires=[
        "dbt-core==1.1.0",
        "setuptools>=40.3.0",
        "impyla>=0.18a5"
    ]
)
