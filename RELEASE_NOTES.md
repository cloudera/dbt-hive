# Changelog

## 1.4.0 (Aug 4th, 2023)
- Support compatibility with dbt-core 1.4.x
- Added functional testing suite and stabilize the main branch.
- Added support for Iceberg table type.
- Added support for Windows platform (using LDAP).
- Bugfix: Handling edge cases for datediff macros.

## 1.3.0 (Nov 25th, 2022)
Now dbt-hive adapter supports dbt-core 1.3.0

## 1.2.0 (Nov 4th, 2022)
Now dbt-hive adapter supports dbt-core 1.2.0

## 1.1.5 (Oct 28th, 2022)
Updated instrumentation schema

## 1.1.4 (Sep 23rd, 2022)
Added support for Kerberos auth mechanism. Along with an updated instrumentation package.

## 1.1.3 (Sep 9th, 2022)
Added a macro to detect the hive version, to determine if the incremental merge is supported by the warehouse.

## 1.1.2 (Sep 2nd, 2022)
dbt seeds command won't add additional quotes to string, which was a known bug in the previous release. All warehouse properties(Cluster_by, Comment, external table, incremental materialization methods, etc) are tested and should be working smoothly with the adapter. Added instrumentation to the adapter

## 1.1.1 (August 23rd, 2022)  
Cloudera released the first version of the dbt-hive adapter
