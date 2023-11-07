# Databricks notebook source
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.join.enabled", "true")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_date, col, first, min, current_date
def new_records(table_name):
    return spark.read.table(table_name).filter(to_date(current_timestamp()) == to_date(col('ingest_time')))

# COMMAND ----------

df_new_records = new_records('data_source1')
df_new_records_2 = new_records('data_source2')

# COMMAND ----------

df_new_records.persist()
df_new_records_2.persist()
