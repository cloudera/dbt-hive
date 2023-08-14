# dbt-hive

The `dbt-hive` adapter allows you to use [dbt](https://www.getdbt.com/) along with [Apache Hive](https://hive.apache.org/) and [Cloudera Data Platform](https://cloudera.com)


## Getting started

- [Install dbt](https://docs.getdbt.com/docs/installation)
- Read the [introduction](https://docs.getdbt.com/docs/introduction/) and [viewpoint](https://docs.getdbt.com/docs/about/viewpoint/)

### Credits

The initial adapter code was developed by bachng2017 who agreed to transfer the ownership and continute active development.
This code base is now being activiely developed and maintained by Cloudera.

### Requirements

Current version of dbt-hive use dbt-core 1.4.*. We are actively working on supporting next version of dbt-core 1.5

Python >= 3.8
dbt-core ~= 1.4.*
impyla >= 0.18

### Install
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
| Name | Supported | Iceberg |
|------|-----------|---------|
|Materialization: View | Yes | N/A |
|Materialization: Table| Yes | Yes |
|Materialization: Table with Partitions | Yes | Yes |
|Materialization: Incremental - Append | Yes | Yes|
|Materialization: Incremental - Append with Partitions | Yes | Yes|
|Materialization: Incremental - Insert+Overwrite| Yes | Yes |
|Materialization: Incremental - Insert+Overwrite with Partitions | Yes | Yes |
|Materialization: Incremental - Merge | No | No |
|Materialization: Ephemeral | No | No |
|Seeds | Yes | Yes |
|Tests | Yes | Yes |
|Snapshots | No | No |
|Documentation | Yes | No |
|Authentication: LDAP | Yes | Yes |
|Authentication: Kerberos | Yes | Yes |

### Incremental

Incremental models are explained in [dbt documentations](https://docs.getdbt.com/docs/build/incremental-models). This section covered the  details about the incremental strategy support by the dbt-hive.

| Strategy | ACID Table | Iceberg Table |
|------|------|---------|
| Incremental Full-Refresh | Yes | Yes |
| Incremental Append | Yes | Yes |
| Incremental Append with Partitions | Yes | Yes |
| Incremental Insert+Overwrite | Yes | Yes|
| Incremental Insert+Overwrite with Partitions | Yes | Yes|
| Incremental Merge | No | Yes |
| Incremental Merge with Partitions | No | Yes |

On-Schema Change Strategy

| Strategy | ACID Table | Iceberg Table |
|------|------|---------|
| ignore | Suported (default)  | Supported (default) |
| fail | Suported | Suported |
| append_new_columns | Append new columns | Append new columns |
| sync_all_columns | Add new columns but doesn't remove existing columns | Add new columns but doesn't remove existing columns |  

### Tests Coverage

#### Functional Tests
| Name | Base | Iceberg |
|------|------|---------|
|Materialization: View | Yes | N/A |
|Materialization: Table| Yes | Yes |
|Materialization: Table with Partitions | Yes | Yes |
|Materialization: Incremental - Append | Yes | Yes|
|Materialization: Incremental - Append with Partitions | Yes | Yes|
|Materialization: Incremental - Insert+Overwrite| Yes | Yes |
|Materialization: Incremental - Insert+Overwrite with Partitions | Yes | Yes |
|Materialization: Incremental - Merge | No | No |
|Materialization: Ephemeral | No | No |
|Seeds | Yes | Yes |
|Tests | Yes | Yes |
|Snapshots | No | No |
|Documentation | Yes | No |
|Authentication: LDAP | Yes | Yes |
|Authentication: Kerberos | Yes | Yes |

Note: Kerberos is only qualified on Unix platform.
