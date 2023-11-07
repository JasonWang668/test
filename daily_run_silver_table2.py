# Databricks notebook source
# MAGIC %run /Users/aini19931225@hotmail.com/configuration_daily_run_reporting_table

# COMMAND ----------

from pyspark.sql.functions import first, last, col, dense_rank,when, current_timestamp,lit
from pyspark.sql.window import Window

def silver_table2_agg(dataframe_source):
    usage_count = dataframe_source.groupBy("user_id","parcel_locker_id").count()
    return usage_count.selectExpr('user_id','parcel_locker_id','count',"'init' as changed_method","current_timestamp() as changed_timestamp")

# COMMAND ----------

df_new_records_agg = silver_table2_agg(df_new_records)
df_new_records_agg.createOrReplaceTempView('silver_table2_updates')

# COMMAND ----------

query = """
merge into silver_table2 as t 
using silver_table2_updates as s 
on t.user_id = s.user_id and t.parcel_locker_id = s.parcel_locker_id
when matched then 
update set t.count = (s.count+t.count), t.changed_method = 'update', t.changed_timestamp = current_timestamp()
when not matched then
insert (user_id, parcel_locker_id, count, changed_method, changed_timestamp)
values(s.user_id, s.parcel_locker_id, s.count, 'append', current_timestamp())
"""

# COMMAND ----------

spark.sql(query)
