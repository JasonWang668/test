# Databricks notebook source
# MAGIC %md
# MAGIC There are following data sources, the sources provide updates once a day.
# MAGIC Data source 1:
# MAGIC - user id
# MAGIC - parcel locker id
# MAGIC - timestamp (of parcel delivery)
# MAGIC
# MAGIC Data source 2:
# MAGIC - user id
# MAGIC - if user consent on marketing communication (boolean)
# MAGIC - timestamp (when user changed consent)
# MAGIC
# MAGIC Please suggest table/tables, for storing following data (and implement function in spark to process the data).
# MAGIC Please focus on performance of querying and updating the data.
# MAGIC Target table should contain following informations:
# MAGIC - user id
# MAGIC - id of parcel locker firstly user by the user
# MAGIC - id of most frequently used parcel locker (by the user)
# MAGIC - id of second most frequently used parcel locker (by the user)
# MAGIC - date of first received parcel by the user
# MAGIC - number of users, which received their first parcel on the same day as the user
# MAGIC - whether the user had marketing consent when received first parcel
# MAGIC - whether the user had marketing consent when report creating

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.join.enabled", "true")

# COMMAND ----------

df_source1 = spark.table('data_source1')
df_source2 = spark.table('data_source2')

# COMMAND ----------

# MAGIC %md
# MAGIC ### init mode

# COMMAND ----------

from pyspark.sql.functions import first, min

silver_table1 = (
    df_source1.groupBy("user_id")
    .agg(
        first("parcel_locker_id").alias("id_parcel_locker_first_used"),
        min("delivery_date").alias("date_first_received_parcel"),
        min("timestamp").alias("timestamp_first_received_parcel")
    ).selectExpr("user_id","id_parcel_locker_first_used","date_first_received_parcel",
                 "timestamp_first_received_parcel","'init' as changed_method", "current_timestamp() as changed_timestamp")
)
silver_table1.write.mode("overwrite").saveAsTable('silver_table1')

# COMMAND ----------

display(silver_table1)

# COMMAND ----------

# MAGIC %sql drop table silver_table2

# COMMAND ----------

from pyspark.sql.functions import first, last, col, dense_rank,when, current_timestamp,lit
from pyspark.sql.window import Window

def silver_table2_agg(dataframe_source):
    usage_count = dataframe_source.groupBy("user_id","parcel_locker_id").count()
    # window specification to rank the usage count
    # window_spec = Window.partitionBy("user_id").orderBy(col("count").desc())
    # silver_table2 = (
    #     usage_count.withColumn("rank", dense_rank().over(window_spec))
    #     .select('user_id','parcel_locker_id','count','rank')
    # )
    return usage_count.selectExpr('user_id','parcel_locker_id','count',"'init' as changed_method","current_timestamp() as changed_timestamp")
silver_table2 = silver_table2_agg(spark.read.option("versionAsOf",0).table('data_source1'))
silver_table2.write.mode("overwrite").option('mergeSchema','true').saveAsTable('silver_table2')

# COMMAND ----------

display(silver_table2)

# COMMAND ----------

df_source1 = df_source1.select([col(c).alias(f"df1_{c}") for c in df_source1.columns])
df_source2 = df_source2.select([col(c).alias(f"df2_{c}") for c in df_source2.columns])

# COMMAND ----------

silver_table1 = spark.read.table("silver_table1")
silver_table1 = silver_table1.select([col(c).alias(f"st1_{c}") for c in silver_table1.columns])

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace table scd_consent_marketing 
# MAGIC as 
# MAGIC select 
# MAGIC   user_id,
# MAGIC   marketing_consent,
# MAGIC   timestamp as start_timestamp,
# MAGIC   LEAD(timestamp,1) over (
# MAGIC     partition by user_id
# MAGIC     order by timestamp
# MAGIC   ) end_timestamp,
# MAGIC   case when end_timestamp is null then 'current' else '' end as flag 
# MAGIC from data_source2 ;

# COMMAND ----------

# MAGIC %sql select * from scd_consent_marketing

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace table silver_table3
# MAGIC as 
# MAGIC select cm.user_id, 
# MAGIC   case when (st1.date_first_received_parcel between to_date(cm.start_timestamp) and coalesce(to_date(cm.end_timestamp), current_date())) 
# MAGIC     then cm.marketing_consent 
# MAGIC     else false end as if_marketing_consent_when_first_parcel, 
# MAGIC   cm.start_timestamp, 
# MAGIC   cm.end_timestamp, 
# MAGIC   st1.date_first_received_parcel, 
# MAGIC   'init' as changed_method, 
# MAGIC   current_timestamp() as changed_timestamp
# MAGIC from scd_consent_marketing cm
# MAGIC left join silver_table1 st1
# MAGIC on cm.user_id = st1.user_id
# MAGIC where (st1.id_parcel_locker_first_used is not null)

# COMMAND ----------

# MAGIC %sql select * from silver_table3

# COMMAND ----------

# MAGIC %md
# MAGIC ### run mode
# MAGIC when we get the new records of daily batch, we must do a incremental update or append to propagate the modification in downstreams. 
# MAGIC
# MAGIC ######- silver_table1 stores firstly used parcel locker id and date. So we can apply 
# MAGIC
# MAGIC 1. append only when new user_id arrives.
# MAGIC 2. update when existed user_id has a ealier date of first used parcel locker.
# MAGIC ######- silver_table2 stores statistic count per user per used parcel locker. So we can apply
# MAGIC 1. append only when new user_id and new parcel_locker_id arrives.
# MAGIC 2. update when existed user_id and parcel_locker_id is still used by the user. 
# MAGIC  
# MAGIC ######- silver_table3 stores if maketing consent when first received parcel, which depends on silver_table1. We do have the same rules to update silver_table3 :
# MAGIC 1. append only when new user_id arrives.  
# MAGIC 2. update when existed user_id has a earlier date of first used parcel locker. 
# MAGIC

# COMMAND ----------

# Define a function to get the new records after daily batch
from pyspark.sql.functions import current_timestamp, to_date
def new_records(table_name):
    return spark.read.table(table_name).filter(to_date(current_timestamp()) == to_date(col('ingest_time')))

# COMMAND ----------

df_new_records = new_records('data_source1')
df_new_records_2 = new_records('data_source2')

# COMMAND ----------

silver_table1 = spark.table('silver_table1')
# get only new user_id 
df_new_user = (df_new_records
    .join(silver_table1, ['user_id'], 'left')
    .filter(col('silver_table1.id_parcel_locker_first_used').isNull())
    .groupBy('user_id')
    .agg(
        first("parcel_locker_id").alias("id_parcel_locker_first_used"),
        min("delivery_date").alias("date_first_received_parcel"),
        min("timestamp").alias("timestamp_first_received_parcel")
    ).select("user_id","id_parcel_locker_first_used","date_first_received_parcel","timestamp_first_received_parcel")
)
df_new_user.write.mode('append').saveAsTable('silver_table1')

# COMMAND ----------

display(df_new_user)

# COMMAND ----------

# get update for existed user_id
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

display(df_existed_user)

# COMMAND ----------

# MAGIC %sql 
# MAGIC merge into silver_table1 as t 
# MAGIC using silver_table1_updates as s 
# MAGIC on t.user_id = s.user_id
# MAGIC when matched then 
# MAGIC update set t.timestamp_first_received_parcel = s.timestamp_first_received_parcel, t.id_parcel_locker_first_used = s.id_parcel_locker_first_used,
# MAGIC   t.date_first_received_parcel = s.date_first_received_parcel;

# COMMAND ----------

df_new_records_agg = silver_table2_agg(df_new_records)
df_new_records_agg.createOrReplaceTempView('silver_table2_updates')

# COMMAND ----------

# MAGIC %sql 
# MAGIC merge into silver_table2 as t 
# MAGIC using silver_table2_updates as s 
# MAGIC on t.user_id = s.user_id and t.parcel_locker_id = s.parcel_locker_id
# MAGIC when matched then 
# MAGIC update set t.count = (s.count+t.count), t.changed_method = 'update', t.changed_timestamp = current_timestamp()
# MAGIC when not matched then
# MAGIC insert (user_id, parcel_locker_id, count, changed_method, changed_timestamp)
# MAGIC values(s.user_id, s.parcel_locker_id, s.count, 'append', current_timestamp())

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace view silver_table3_upserts
# MAGIC as 
# MAGIC with silver_table1_new_records as (
# MAGIC   select * from silver_table1 where (changed_method == 'update' or changed_method == 'append') and to_date(changed_timestamp) == current_date()
# MAGIC )
# MAGIC select cm.user_id, 
# MAGIC   case when (st1.date_first_received_parcel between to_date(cm.start_timestamp) and coalesce(to_date(cm.end_timestamp), current_date())) 
# MAGIC     then cm.marketing_consent 
# MAGIC     else false end as if_marketing_consent_when_first_parcel, 
# MAGIC   cm.start_timestamp, 
# MAGIC   cm.end_timestamp 
# MAGIC from scd_consent_marketing cm
# MAGIC left join silver_table1_new_records st1
# MAGIC on cm.user_id = st1.user_id
# MAGIC where (st1.id_parcel_locker_first_used is not null)

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into silver_table3 as t
# MAGIC using silver_table3_upserts s
# MAGIC on t.user_id = s.user_id
# MAGIC when matched then 
# MAGIC update set t.if_marketing_consent_when_first_parcel = s.if_marketing_consent_when_first_parcel, t.start_timestamp=s.start_timestamp,
# MAGIC     t.end_timestamp = s.end_timestamp,  t.changed_method = 'update', t.changed_timestamp = current_timestamp()
# MAGIC when not matched then 
# MAGIC insert (user_id, if_marketing_consent_when_first_parcel, start_timestamp, end_timestamp,changed_method, changed_timestamp )
# MAGIC values (s.user_id, s.if_marketing_consent_when_first_parcel, s.start_timestamp, s.end_timestamp, 'append', current_timestamp())
