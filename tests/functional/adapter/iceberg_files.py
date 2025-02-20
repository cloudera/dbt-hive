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

merge_iceberg_sql = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy='merge',
    merge_exclude_columns=['msg'],
    table_type='iceberg'
) }}

{% if not is_incremental() %}

-- data for first invocation of model

select 1 as id, 'hello' as msg, 'blue' as color
union all
select 2 as id, 'goodbye' as msg, 'red' as color

{% else %}

-- data for subsequent incremental update

select 1 as id, 'hey' as msg, 'blue' as color
union all
select 2 as id, 'yo' as msg, 'green' as color
union all
select 3 as id, 'anyway' as msg, 'purple' as color

{% endif %}
"""

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
