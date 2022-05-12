#!/usr/bin/env python
from setuptools import find_namespace_packages, setup

package_name = "dbt-hive"
# make sure this always matches dbt/adapters/hive/__version__.py
package_version = "0.20.0rc1"
description = """The hive adapter plugin for dbt"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author='Nguyen Huu Bach',
    author_email='bachng@gmail.com',
    url='https://www.ntt.com',
    packages=find_namespace_packages(include=['dbt', 'dbt.*']),
    include_package_data=True,
    install_requires=[
        "dbt-core==0.20.0"
    ]
)
