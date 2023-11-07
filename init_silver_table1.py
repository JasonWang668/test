# Databricks notebook source
df_source1 = spark.table('data_source1')

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


