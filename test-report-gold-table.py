# Databricks notebook source
# MAGIC %sql describe history data_source1

# COMMAND ----------

# MAGIC %sql delete from silver_table3 where changed_method = 'append'

# COMMAND ----------

# MAGIC %sql select * from data_source1 version as of 0

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from silver_table1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_table2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_table3

# COMMAND ----------

# MAGIC %sql select * from scd_consent_marketing

# COMMAND ----------

# MAGIC %sql select * from gold_reporting_table

# COMMAND ----------

st1 = spark.read.table('silver_table1')
st2 = spark.read.table('silver_table2')
st3 = spark.read.table('silver_table3')
scd_consent_marketing = spark.read.table('scd_consent_marketing')

# COMMAND ----------

# calculate number of users, which received their first parcel on the same day as the user 
num_users_when_first_parcel = (
    st1
    .groupBy('date_first_received_parcel')
    .count()
    .selectExpr('date_first_received_parcel', 'count as num_users_when_first_parcel'))
num_users_when_first_parcel.createOrReplaceTempView('num_users_when_first_parcel')

# COMMAND ----------

from pyspark.sql.functions import col
marketing_consent_when_reporting = scd_consent_marketing.filter(col('flag') == 'current').select('user_id','marketing_consent').createOrReplaceTempView('marketing_consent_when_reporting')

# COMMAND ----------

# MAGIC %md
# MAGIC ### init mode

# COMMAND ----------

# calculate the most frequently used parcel locker and second most frequently used parcel locker based on silver_table2
from pyspark.sql.functions import first, last, col, dense_rank,when
from pyspark.sql.window import Window

df_user_id_updatedOrAppened = st2.filter(col('changed_method') == 'init').select('user_id').distinct().persist()
# window specification to rank the usage count
window_spec = Window.partitionBy("user_id").orderBy(col("count").desc())
df_first_second_pl = (
    st2.join(df_user_id_updatedOrAppened, ['user_id'], 'inner').withColumn("rank", dense_rank().over(window_spec))
    .filter( (col("rank") == 1) | (col("rank") == 2) )
    .groupBy("silver_table2.user_id")
    .pivot("rank")
    .agg(
        first("parcel_locker_id").alias("most_frequent_pl"),
        last("parcel_locker_id").alias("second_most_frequent_pl")
    )
    .withColumn(
        "second_most_frequent_pl",
        when(col("1_most_frequent_pl") == col("1_second_most_frequent_pl"), None).otherwise(col("1_second_most_frequent_pl"))
    )
    .selectExpr("user_id","1_most_frequent_pl as most_frequent_pl","second_most_frequent_pl")
)
df_first_second_pl.createOrReplaceTempView("first_second_used_pl")

# COMMAND ----------

# MAGIC %sql select * from first_second_used_pl

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace table gold_reporting_table as
# MAGIC select 
# MAGIC st1.user_id,
# MAGIC st1.id_parcel_locker_first_used,
# MAGIC st1.date_first_received_parcel,
# MAGIC st2.most_frequent_pl,
# MAGIC st2.second_most_frequent_pl,
# MAGIC st3.if_marketing_consent_when_first_parcel,
# MAGIC nuwfp.num_users_when_first_parcel,
# MAGIC mcwr.marketing_consent as if_marketing_consent_when_reporting
# MAGIC from silver_table1 st1
# MAGIC join first_second_used_pl st2
# MAGIC on st1.user_id = st2.user_id
# MAGIC join silver_table3 st3 
# MAGIC on st1.user_id = st3.user_id
# MAGIC join num_users_when_first_parcel nuwfp
# MAGIC on st1.date_first_received_parcel = nuwfp.date_first_received_parcel
# MAGIC join marketing_consent_when_reporting mcwr
# MAGIC on st1.user_id = mcwr.user_id ; 

# COMMAND ----------

# MAGIC %sql select * from gold_reporting_table

# COMMAND ----------

# MAGIC %md
# MAGIC ### run mode
# MAGIC get the updates or appends from silver_table1, silver_table2, silver_table3 and merge them into gold_reporting_table

# COMMAND ----------

from pyspark.sql.functions import col, to_date, current_date
st1_new_batch = st1.filter(
    (col('changed_method') == 'update' )
    | (col('changed_method') == 'append')
    & (to_date(col('changed_timestamp')) ==  current_date() )
    ).createOrReplaceTempView('temp_silver_table1_new_batch')
st3_new_batch = st3.filter(
    (col('changed_method') == 'update' )
    | (col('changed_method') == 'append')
    & (to_date(col('changed_timestamp')) ==  current_date() )
    ).createOrReplaceTempView('temp_silver_table3_new_batch')

# COMMAND ----------

# with incremental way, to calculate the most frequently used parcel locker and second most frequently used parcel locker based on silver_table2
from pyspark.sql.functions import first, last, col, dense_rank,when
from pyspark.sql.window import Window

df_user_id_updatedOrAppened = st2.filter(
    (col('changed_method') == 'update' )
    | (col('changed_method') == 'append')
    & (to_date(col('changed_timestamp')) ==  current_date() )
    ).select('user_id').distinct().persist()
if not df_user_id_updatedOrAppened.isEmpty():
    # window specification to rank the usage count
    window_spec = Window.partitionBy("user_id").orderBy(col("count").desc())
    df_first_second_pl = (
        st2.join(df_user_id_updatedOrAppened, ['user_id'], 'inner')
        .withColumn("rank", dense_rank().over(window_spec))
        .filter( (col("rank") == 1) | (col("rank") == 2) )
        .groupBy("silver_table2.user_id")
        .pivot("rank")
        .agg(
            first("parcel_locker_id").alias("most_frequent_pl"),
            last("parcel_locker_id").alias("second_most_frequent_pl")
        )
        .withColumn(
            "second_most_frequent_pl",
            when(col("1_most_frequent_pl") == col("1_second_most_frequent_pl"), None).otherwise(col("1_second_most_frequent_pl"))
        )
        .selectExpr("user_id","1_most_frequent_pl as most_frequent_pl","second_most_frequent_pl")
    )
    df_first_second_pl.createOrReplaceTempView("first_second_used_pl_new_batch")
else:
    spark.createDataFrame([], "user_id string, most_frequent_pl string, seconde_most_frequent_pl string").createOrReplaceTempView("first_second_used_pl_new_batch")

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into gold_reporting_table t
# MAGIC using (
# MAGIC   select 
# MAGIC   st1.user_id,
# MAGIC   st1.id_parcel_locker_first_used,
# MAGIC   st1.date_first_received_parcel,
# MAGIC   st2.most_frequent_pl,
# MAGIC   st2.second_most_frequent_pl,
# MAGIC   st3.if_marketing_consent_when_first_parcel,
# MAGIC   nuwfp.num_users_when_first_parcel,
# MAGIC   mcwr.marketing_consent as if_marketing_consent_when_reporting
# MAGIC   from temp_silver_table1_new_batch st1
# MAGIC   join first_second_used_pl st2
# MAGIC   on st1.user_id = st2.user_id
# MAGIC   join silver_table3 st3 
# MAGIC   on st1.user_id = st3.user_id
# MAGIC   join num_users_when_first_parcel nuwfp
# MAGIC   on st1.date_first_received_parcel = nuwfp.date_first_received_parcel
# MAGIC   join marketing_consent_when_reporting mcwr
# MAGIC   on st1.user_id = mcwr.user_id ; 
# MAGIC ) as s
# MAGIC when matched then 
# MAGIC update set 
# MAGIC   t.id_parcel_locker_first_used = s.id_parcel_locker_first_used,
# MAGIC   t.date_first_received_parcel = s.date_first_received_parcel,
# MAGIC   t.most_frequent_pl = s.most_frequent_pl,
# MAGIC   t.second_most_frequent_pl = s.second_most_frequent_pl,
# MAGIC   t.if_marketing_consent_when_first_parcel = s.if_marketing_consent_when_first_parcel,
# MAGIC   t.num_users_when_first_parcel = s.num_users_when_first_parcel,
# MAGIC   t.if_marketing_consent_when_reporting = s.if_marketing_consent_when_reporting
# MAGIC when not matched then 
# MAGIC insert *
