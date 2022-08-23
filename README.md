# dbt-hive

The `dbt-hive` adapter allows you to use [dbt](https://www.getdbt.com/) along with [Apache Hive](https://hive.apache.org/) and [Cloudera Data Platform](https://cloudera.com)


## Getting started

- [Install dbt](https://docs.getdbt.com/docs/installation)
- Read the [introduction](https://docs.getdbt.com/docs/introduction/) and [viewpoint](https://docs.getdbt.com/docs/about/viewpoint/)

### Credits

The initial adapter code was developed by bachng2017 who agreed to transfer the ownership and continute active development.
This code base is now being activiely developed and maintained by Innovation Accelerator team in Cloudera.

### Requirements

Python >= 3.8

dbt-core ~= 1.1.0

impyla >= 0.18a5

### Install
Clone the repository
```
cd dbt-hive
pip3 install --user .
```
or install from pypip
```
pip3 install --user dbt-hive
```

### Sample profile
```
demo_project:
  target: dev
  outputs:
  dev:
    type: hive
    auth_type: LDAP
    user: [username]
    password: [password]
    schema: [schema]
    host: [hive-meta-store-host]
    port: 443
    http_path: [http-path]
    thread: 1
```

## Supported features
| Name | Supported |
|------|-----------|
|Materialization: Table|Yes|
|Materialization: View|Yes|
|Materialization: Incremental - Append|Yes|
|Materialization: Incremental - Insert+Overwrite|Yes|
|Materialization: Incremental - Merge|No|
|Materialization: Ephemeral|No|
|Seeds|Yes|
|Tests|Yes|
|Snapshots|No|
|Documentation|Yes|
|Authentication: LDAP|Yes|
|Authentication: Kerberos|No|

