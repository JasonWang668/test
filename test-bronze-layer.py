# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE test_lab_wwb;
# MAGIC USE test_lab_wwb;

# COMMAND ----------

# MAGIC %fs mkdirs dbfs:/Users/weibin.wang/test_lab_wwb/bronze

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/Users/weibin.wang/test_lab_wwb/bronze/

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a Delta Lake table for Data Source 1
# MAGIC DROP TABLE if EXISTS data_source1 ;
# MAGIC CREATE OR REPLACE TABLE data_source1
# MAGIC     (user_id INT, parcel_locker_id STRING, delivery_timestamp TIMESTAMP, delivery_date DATE, year_month STRING)
# MAGIC USING delta
# MAGIC PARTITIONED BY (year_month)
# MAGIC LOCATION 'dbfs:/Users/weibin.wang/test_lab_wwb/bronze/data_source1'
# MAGIC TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO data_source1 (user_id, parcel_locker_id, delivery_timestamp, delivery_date, year_month)
# MAGIC VALUES
# MAGIC   (1, 'A123', '2023-01-01 08:00:00', '2023-01-01', '202301'),
# MAGIC   (2, 'B456', '2023-01-01 09:30:00', '2023-01-01', '202301'),
# MAGIC   (3, 'C789', '2023-01-02 11:15:00', '2023-01-02', '202301');
# MAGIC SELECT * FROM data_source1;

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/Users/weibin.wang/test_lab_wwb/bronze/data_source2

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a Delta Lake table for Data Source 2
# MAGIC DROP TABLE IF EXISTS data_source2;
# MAGIC CREATE OR REPLACE TABLE data_source2
# MAGIC     (user_id INT, marketing_consent BOOLEAN, consent_timestamp TIMESTAMP, consent_date DATE,year_month STRING)
# MAGIC USING delta
# MAGIC PARTITIONED BY (year_month)
# MAGIC LOCATION 'dbfs:/Users/weibin.wang/test_lab_wwb/bronze/data_source2'
# MAGIC TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO data_source2 (
# MAGIC     user_id,
# MAGIC     marketing_consent,
# MAGIC     consent_timestamp,
# MAGIC     consent_date,
# MAGIC     year_month)
# MAGIC VALUES
# MAGIC     (1, true, '2023-01-15 08:00:00', '2023-01-15','202301'),
# MAGIC     (2, false, '2023-01-15 09:00:00', '2023-01-15','202301'),
# MAGIC     (3, true, '2023-01-17 12:30:00', '2023-01-17','202301');
# MAGIC SELECT * FROM data_source2;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history data_source1

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe history data_source2

# COMMAND ----------

# MAGIC %md
# MAGIC Or if the files of data source1 and data source2 will be sent to the directory of cloud, I prefer the autoloader to ingest these files.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType
from pyspark.sql.functions import col, year, month, to_date

schema_source1 = StructType([
    StructField("user_id", StringType(), True),
    StructField("parcel_locker_id", StringType(), True),
    StructField("delivery_timestamp", TimestampType(), True)
])
file_path_source1 = "dbfs:/Users/weibin.wang/test_lab_wwb/incoming/bronze/data_source1/"

schema_source2 = StructType([
    StructField("user_id", StringType(), True),
    StructField("marketing_consent", BooleanType(), True),
    StructField("consent_timestamp", TimestampType(), True)
])
file_path_source2 = "dbfs:/Users/weibin.wang/test_lab_wwb/incoming/bronze/data_source2/"

def load_data_source1():
    query = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","json")
        .schema(schema_source1)
        .load(file_path_source1)
        .withColumn("year_month",year("delivery_timestamp").cast("string") + month("delivery_timestamp").cast("string"))
        .withColumn("delivery_date", to_date(col("delivery_timestamp")))
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "dbfs:/Users/weibin.wang/test_lab_wwb/bronze/data_source1/checkpoint")
        #.option("mergeSchema","true")
        .partitionBy("year_month")
        .trigger(availableNow=True)
        .table("data_source1")
    )
    query.awaitTermination()


def load_data_source2():
    query = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","json")
        .schema(schema_source2)
        .load(file_path_source2)
        .withColumn("year_month",year("consent_timestamp").cast("string") + month("consent_timestamp").cast("string"))
        .withColumn("consent_date", to_date(col("consent_timestamp")))
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "dbfs:/Users/weibin.wang/test_lab_wwb/bronze/data_source2/checkpoint")
        #.option("mergeSchema","true")
        .partitionBy("year_month")
        .trigger(availableNow=True)
        .table("data_source2")
    )
    query.awaitTermination()

# COMMAND ----------

load_data_source1()

# COMMAND ----------

load_data_source2()

# COMMAND ----------


