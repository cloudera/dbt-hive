Hive dbt adapter

### 
Hive adapter for dbt. https://www.getdbt.com/

- supported features:
   - base
   - ephemeral
   - incremntal

- not supported features:
   - snapshot

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
hive-exp:
  target: dev
  outputs:
  dev:
    type: hive
    auth_type: LDAP
    user: [username]
    password: [password]
    schema: dbtdemo
    host: [hive-meta-store-host]
    port: 443
    http_path: [http-path]
    thread: 1
```
