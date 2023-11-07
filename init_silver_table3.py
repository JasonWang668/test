# Databricks notebook source
query = """
create or replace table silver_table3
as 
select cm.user_id, 
  case when (st1.date_first_received_parcel between to_date(cm.start_timestamp) and coalesce(to_date(cm.end_timestamp), current_date())) 
    then cm.marketing_consent 
    else false end as if_marketing_consent_when_first_parcel, 
  cm.start_timestamp, 
  cm.end_timestamp, 
  st1.date_first_received_parcel, 
  'init' as changed_method, 
  current_timestamp() as changed_timestamp
from scd_consent_marketing cm
left join silver_table1 st1
on cm.user_id = st1.user_id
where (st1.id_parcel_locker_first_used is not null);
"""

# COMMAND ----------

spark.sql(query)
