Hive3 dbt adapter (experimental)

### 
Hive adapter for dbt. https://www.getdbt.com/

Currently, only support Hive3

- support features:
   - base
   - ephemeral
   - incremntal

- not support features:
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
       auth: LDAP
       user: "{{ env_var('HIVE_USER') }}"
       password: "{{ env_var('HIVE_PASSWORD') }}"
       schema: my_schema
       host: 127.0.0.1
```
