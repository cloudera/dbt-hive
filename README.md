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
```
cd dbt-hive
python3 setup.py clean
python3 setup.py install --user
pip3 list installed | grep dbt-hive
```

### Sample profile
```
dlx-hive:
  target: dev
  outputs:
     dev:
       type: hive
       auth: LDAP
       user: "{{ env_var('HIVE_USER') }}"
       password: "{{ env_var('HIVE_PASSWORD') }}"
       schema: my_schema
       host: 172.31.0.16
```
