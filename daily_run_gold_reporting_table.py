# Databricks notebook source
# MAGIC %run /Users/aini19931225@hotmail.com/configuration_daily_run_reporting_table

# COMMAND ----------

st1 = spark.read.table('silver_table1')
st2 = spark.read.table('silver_table2')
st3 = spark.read.table('silver_table3')
scd_consent_marketing = spark.read.table('scd_consent_marketing')

# COMMAND ----------

num_users_when_first_parcel = (
    st1
    .groupBy('date_first_received_parcel')
    .count()
    .selectExpr('date_first_received_parcel', 'count as num_users_when_first_parcel'))
num_users_when_first_parcel.createOrReplaceTempView('num_users_when_first_parcel')

# COMMAND ----------

marketing_consent_when_reporting = scd_consent_marketing.filter(col('flag') == 'current').select('user_id','marketing_consent').createOrReplaceTempView('marketing_consent_when_reporting')

# COMMAND ----------

st1_new_batch = st1.filter(
    (col('changed_method').isin('update','append') )
    & (to_date(col('changed_timestamp')) ==  current_date() )
    ).createOrReplaceTempView('temp_silver_table1_new_batch')
st3_new_batch = st3.filter(
    (col('changed_method').isin('update','append') )
    & (to_date(col('changed_timestamp')) ==  current_date() )
    ).createOrReplaceTempView('temp_silver_table3_new_batch')

# COMMAND ----------

from pyspark.sql.functions import first, last, col, dense_rank,when
from pyspark.sql.window import Window

df_user_id_updatedOrAppened = st2.filter(
    (col('changed_method') == 'update' )
    | (col('changed_method') == 'append')
    & (to_date(col('changed_timestamp')) ==  current_date() )
    ).select('user_id').distinct().persist()
if not df_user_id_updatedOrAppened.isEmpty():

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
    spark.createDataFrame([], "user_id string, most_frequent_pl string, second_most_frequent_pl string").createOrReplaceTempView("first_second_used_pl_new_batch")

# COMMAND ----------

query_merge = """
merge into gold_reporting_table t
using (
  select 
  st1.user_id,
  st1.id_parcel_locker_first_used,
  st1.date_first_received_parcel,
  st2.most_frequent_pl,
  st2.second_most_frequent_pl,
  st3.if_marketing_consent_when_first_parcel,
  nuwfp.num_users_when_first_parcel,
  mcwr.marketing_consent as if_marketing_consent_when_reporting
  from temp_silver_table1_new_batch st1
  join first_second_used_pl_new_batch st2
  on st1.user_id = st2.user_id
  join temp_silver_table3_new_batch st3 
  on st1.user_id = st3.user_id
  join num_users_when_first_parcel nuwfp
  on st1.date_first_received_parcel = nuwfp.date_first_received_parcel
  join marketing_consent_when_reporting mcwr
  on st1.user_id = mcwr.user_id 
) as s
on t.user_id = s.user_id
when matched then 
update set 
  t.id_parcel_locker_first_used = s.id_parcel_locker_first_used,
  t.date_first_received_parcel = s.date_first_received_parcel,
  t.most_frequent_pl = s.most_frequent_pl,
  t.second_most_frequent_pl = s.second_most_frequent_pl,
  t.if_marketing_consent_when_first_parcel = s.if_marketing_consent_when_first_parcel,
  t.num_users_when_first_parcel = s.num_users_when_first_parcel,
  t.if_marketing_consent_when_reporting = s.if_marketing_consent_when_reporting
when not matched then 
insert *
"""

# COMMAND ----------

spark.sql(query_merge)
