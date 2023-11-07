# Databricks notebook source
# MAGIC %run /Users/aini19931225@hotmail.com/configuration_daily_run_reporting_table

# COMMAND ----------

silver_table1_new_records = (spark.read.table('silver_table1')
                            .filter( col('changed_method').isin('update','append') & (to_date(col('changed_timestamp')) == current_date()) )
)
silver_table1_new_records.createOrReplaceTempView('silver_table1_new_records')

# COMMAND ----------

query_get_upserts = """
select cm.user_id, 
  case when (st1.date_first_received_parcel between to_date(cm.start_timestamp) and coalesce(to_date(cm.end_timestamp), current_date())) 
    then cm.marketing_consent 
    else false end as if_marketing_consent_when_first_parcel, 
  cm.start_timestamp, 
  cm.end_timestamp,
  st1.date_first_received_parcel
from scd_consent_marketing cm
left join silver_table1_new_records st1
on cm.user_id = st1.user_id
where (st1.id_parcel_locker_first_used is not null)
"""

# COMMAND ----------

spark.sql(query_get_upserts).createOrReplaceTempView('silver_table3_upserts')

# COMMAND ----------

query_merge = """
merge into silver_table3 as t
using silver_table3_upserts s
on t.user_id = s.user_id
when matched then 
update set t.if_marketing_consent_when_first_parcel = s.if_marketing_consent_when_first_parcel, 
t.start_timestamp=s.start_timestamp,
t.end_timestamp = s.end_timestamp, 
t.date_first_received_parcel = s.date_first_received_parcel,
t.changed_method = 'update', 
t.changed_timestamp = current_timestamp()
when not matched then 
insert (user_id, if_marketing_consent_when_first_parcel, start_timestamp, end_timestamp, date_first_received_parcel, changed_method, changed_timestamp )
values (s.user_id, s.if_marketing_consent_when_first_parcel, s.start_timestamp, s.end_timestamp, s.date_first_received_parcel, 'append', current_timestamp())
"""

# COMMAND ----------

spark.sql(query_merge)
