-- depends_on: {{ ref('stg_source') }}
-- depends_on: {{ ref('stg_target') }}
{% if execute %}
{% set results = run_query("explain " ~ merge_table()) %}
{{ log("The result of explain command is: " ~ results.print_table(max_rows=None,max_column_width=2000) ~ "") }}
{% endif %}

with temptable as (

     select * from explain_merge.stg_target
)

select * from temptable
