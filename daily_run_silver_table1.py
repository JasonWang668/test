# Databricks notebook source
# MAGIC %run /Users/aini19931225@hotmail.com/configuration_daily_run_reporting_table

# COMMAND ----------

silver_table1 = spark.table('silver_table1')

df_new_user = (df_new_records
    .join(silver_table1, ['user_id'], 'left')
    .filter(col('silver_table1.id_parcel_locker_first_used').isNull())
    .groupBy('user_id')
    .agg(
        first("parcel_locker_id").alias("id_parcel_locker_first_used"),
        min("delivery_date").alias("date_first_received_parcel"),
        min("timestamp").alias("timestamp_first_received_parcel")
    ).selectExpr("user_id","id_parcel_locker_first_used","date_first_received_parcel","timestamp_first_received_parcel", "'append' as changed_method", "current_timestamp() as changed_timestamp")
)
df_new_user.write.mode('append').saveAsTable('silver_table1')

# COMMAND ----------

df_existed_user = (df_new_records
    .join(silver_table1, ['user_id'], 'inner')
    .filter(col('data_source1.delivery_date') < col('silver_table1.date_first_received_parcel'))
    .groupBy('user_id')
    .agg(
        first("parcel_locker_id").alias("id_parcel_locker_first_used"),
        min("delivery_date").alias("date_first_received_parcel"),
        min("timestamp").alias("timestamp_first_received_parcel")
    ).select("user_id","id_parcel_locker_first_used","date_first_received_parcel","timestamp_first_received_parcel")
)
df_existed_user.createTempView('silver_table1_updates')

# COMMAND ----------

query = """
merge into silver_table1 as t 
using silver_table1_updates as s 
on t.user_id = s.user_id
when matched then 
update set t.timestamp_first_received_parcel = s.timestamp_first_received_parcel, t.id_parcel_locker_first_used = s.id_parcel_locker_first_used,
  t.date_first_received_parcel = s.date_first_received_parcel, t.changed_timestamp = current_timestamp(), t.changed_method = 'update'
"""

# COMMAND ----------

spark.sql(query)
