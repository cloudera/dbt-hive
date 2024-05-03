# dbt-hive

The `dbt-hive` adapter allows you to use [dbt](https://www.getdbt.com/) along with [Apache Hive](https://hive.apache.org/) and [Cloudera Data Platform](https://cloudera.com)


## Getting started

- [Install dbt](https://docs.getdbt.com/docs/installation)
- Read the [introduction](https://docs.getdbt.com/docs/introduction/) and [viewpoint](https://docs.getdbt.com/docs/about/viewpoint/)

### Credits

The initial adapter code was developed by bachng2017 who agreed to transfer the ownership and continue active development.
This code base is now being actively developed and maintained by Cloudera.

### Requirements

Current version of dbt-hive use dbt-core 1.7.*. We are actively working on supporting the next version of dbt-core 1.8

Python >= 3.8
dbt-core ~= 1.7.*
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
|Materialization: Incremental - Insert+Overwrite with Partitions | Yes | No |
|Materialization: Incremental - Merge | Yes | Yes |
|Materialization: Incremental - Merge with Partitions | No | Yes* |
|Materialization: Ephemeral | No | No |
|Seeds | Yes | Yes |
|Tests | Yes | Yes |
|Snapshots | No | No |
|Documentation | Yes | No |
|Authentication: LDAP | Yes | Yes |
|Authentication: Kerberos | Yes | Yes |

### Incremental

Incremental models are explained in [dbt documentation](https://docs.getdbt.com/docs/build/incremental-models). This section covered the details about the incremental strategy supported by the dbt-hive.

| Strategy | ACID Table | Iceberg Table |
|------|------|---------|
| Incremental Full-Refresh | Yes | Yes |
| Incremental Append | Yes | Yes |
| Incremental Append with Partitions | Yes | Yes |
| Incremental Insert Overwrite | Not recommended without Partitions* | Not recommended without Partitions* |
| Incremental Insert Overwrite with Partitions | Yes | No |
| Incremental Merge | Yes | Yes* (only v2) |
| Incremental Merge with Partitions | No* | Yes* (only v2) |

**Note***:
1. Incremental Insert overwrite without the partition columns results into completely overwriting the full table and may result in the data-loss. Hence it is not recommended to used. This can happen for Hive ACID, Iceberg v1 & v2 tables.
1. Incremental Merge for iceberg v1 table is not supported because Iceberg v1 tables are not transactional.
1. Incremental Merge with partition columns is not supported because Hive ACID tables doesn't support updating values of partition columns.


Support for [On-Schema Change](https://docs.getdbt.com/docs/build/incremental-models#what-if-the-columns-of-my-incremental-model-change) strategy in dbt-hive:

| Strategy | ACID Table | Iceberg Table |
|------|------|---------|
| ignore (default)  | Supported  | Supported |
| fail | Supported | Supported |
| append_new_columns | Adds new columns | Adds new columns |
| sync_all_columns | Adds new columns and updates datatypes but doesn't remove existing columns | Adds new columns, updates datatypes and removes existing columns  |  

### Tests Coverage

#### Functional Tests
| Name | Base | Iceberg |
|------|------|---------|
|Materialization: View | Yes | N/A |
|Materialization: Table| Yes | Yes |
|Materialization: Table with Partitions | Yes | Yes |
|Materialization: Incremental - Append | Yes | Yes|
|Materialization: Incremental - Append with Partitions | Yes | Yes|
|Materialization: Incremental - Insert+Overwrite with Partitions | Yes | No |
|Materialization: Incremental - Merge | No | No |
|Materialization: Ephemeral | No | No |
|Seeds | Yes | Yes |
|Tests | Yes | Yes |
|Snapshots | No | No |
|Documentation | Yes | No |
|Authentication: LDAP | Yes | Yes |
|Authentication: Kerberos | Yes | Yes |

**Note**: Kerberos is only qualified on Unix platform.
