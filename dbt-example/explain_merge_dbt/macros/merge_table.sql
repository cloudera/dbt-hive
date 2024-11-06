{% macro merge_table() %}
 MERGE INTO explain_merge.stg_target AS T
 USING explain_merge.stg_source AS S ON T.ID = S.ID and T.tran_date = S.tran_date
 WHEN MATCHED AND (T.tran_value != S.tran_value AND S.tran_value IS NOT NULL) THEN UPDATE SET tran_value = S.tran_value, last_updated_user = 'merge_update'
 WHEN MATCHED AND S.tran_value IS NULL THEN DELETE
 WHEN NOT MATCHED THEN INSERT VALUES (S.ID, S.tran_value, 'merge_insert', S.tran_date)
{% endmacro %}
