insertoverwrite_sql = """
{{ config(materialized="incremental", incremental_strategy="insert_overwrite", partition_by="id_partition") }}
select *, id as id_partition from {{ source('raw', 'seed') }}
    {% if is_incremental() %}
        where id > (select max(id) from {{ this }})
    {% endif %}
""".strip()
