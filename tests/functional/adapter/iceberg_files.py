from dbt.tests.adapter.basic.files import model_base, model_incremental

iceberg_base_table_sql = (
    """
{{
  config(
    materialized="table",
    table_type="iceberg"
  )
}}""".strip()
    + model_base
)

iceberg_base_materialized_var_sql = (
    """
{{
  config(
    materialized=var("materialized_var", "table"),
    table_type="iceberg"
  )
}}""".strip()
    + model_base
)

incremental_iceberg_sql = (
    """
 {{
    config(
        materialized="incremental",
        table_type="iceberg"
    )
}}
""".strip()
    + model_incremental
)


incremental_partition_iceberg_sql = """
 {{
    config(
        materialized="incremental",
        partition_by="id",
        table_type="iceberg"
    )
}}
select *, id as id_partition1 from {{ source('raw', 'seed') }}
{% if is_incremental() %}
    where id > (select max(id) from {{ this }})
{% endif %}
""".strip()

incremental_multiple_partition_iceberg_sql = """
 {{
    config(
        materialized="incremental",
        partition_by=["id_partition1", "id_partition2"],
        table_type="iceberg"
    )
}}
select *, id as id_partition1, id as id_partition2 from {{ source('raw', 'seed') }}
{% if is_incremental() %}
    where id > (select max(id) from {{ this }})
{% endif %}
""".strip()

insertoverwrite_iceberg_sql = """
 {{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by="id_partition1",
        table_type="iceberg"
    )
}}
select *, id as id_partition1 from {{ source('raw', 'seed') }}
{% if is_incremental() %}
    where id > (select max(id) from {{ this }})
{% endif %}
""".strip()
