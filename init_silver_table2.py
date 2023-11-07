# Databricks notebook source
df_source1 = spark.table('data_source1')

# COMMAND ----------

from pyspark.sql.functions import first, last, col, dense_rank,when, current_timestamp,lit
from pyspark.sql.window import Window

def silver_table2_agg(dataframe_source):
    usage_count = dataframe_source.groupBy("user_id","parcel_locker_id").count()
    return usage_count.selectExpr('user_id','parcel_locker_id','count',"'init' as changed_method","current_timestamp() as changed_timestamp")
silver_table2 = silver_table2_agg(df_source1)
silver_table2.write.mode("overwrite").option('mergeSchema','true').saveAsTable('silver_table2')
