#!/usr/bin/env python
import os
import re
from setuptools import find_namespace_packages, setup

# pull long description from README
this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md'), 'r', encoding='utf8') as f:
    long_description = f.read()

# get this package's version from dbt/adapters/<name>/__version__.py
def _get_plugin_version_dict():
    _version_path = os.path.join(
        this_directory, 'dbt', 'adapters', 'hive', '__version__.py'
    )
    _semver = r'''(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)'''
    _pre = r'''((?P<prekind>a|b|rc)(?P<pre>\d+))?'''
    _version_pattern = fr'''version\s*=\s*["']{_semver}{_pre}["']'''
    with open(_version_path) as f:
        match = re.search(_version_pattern, f.read().strip())
        if match is None:
            raise ValueError(f'invalid version at {_version_path}')
        return match.groupdict()


# require a compatible minor version (~=), prerelease if this is a prerelease
def _get_dbt_core_version():
    parts = _get_plugin_version_dict()
    minor = "{major}.{minor}.0".format(**parts)
    pre = (parts["prekind"]+"1" if parts["prekind"] else "")
    return f"{minor}{pre}"

package_name = "dbt-hive"
# make sure this always matches dbt/adapters/hive/__version__.py
package_version = "1.1.0b3"
description = """The experient hive adapter plugin for dbt"""

dbt_core_version = _get_dbt_core_version()

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=long_description,
    long_description_content_type='text/markdown',
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
        'dbt-core~={}'.format(dbt_core_version),
        "setuptools>=40.3.0",
        "impyla>=0.18a5"
    ]
)
