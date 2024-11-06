{{
    config(
        stored_as='ORC',
        clustered_by=['id'],
        partition_by=['tran_date'],
        tbl_properties = {'transactional':'true'}
    )
}}

with target_source as (

      select * from explain_merge.raw_target

),
final as (

 select
   id as id,
   t_value as tran_value,
   last_u_user as last_updated_user,
   t_date as tran_date
 from target_source
)
select * from final
