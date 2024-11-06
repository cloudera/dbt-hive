{{
    config(
        stored_as='ORC'
    )
}}

with source as (

      select * from explain_merge.raw_source

),
final as (

 select
   id as id,
   t_value as tran_value,
   t_date as tran_date
 from source
)
select * from final
