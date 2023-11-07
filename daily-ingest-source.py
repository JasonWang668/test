# Databricks notebook source
from pyspark.sql.functions import current_timestamp, input_file_name, col, year, month, day, to_date
from pyspark.sql.column import Column

# Define the function to ingest daily batch data_source1 
def ingest_dailly_batch_source1(time_col:Column, year:int, month:int, day:int):
    (
        spark.read
            .format("parquet")
            .load(f"/mnt/daily_batch/data_source1/{year}/{month:02d}/{day:02d}")
            .select("*",  time_col.alias("ingest_time"), input_file_name().alias("source_file"))
            .withColumn("delivery_date",to_date("timestamp") )
            .write
            .mode("append")
            .saveAsTable("data_source1")
    )
# Define the function to ingest daily batch data_source2
def ingest_dailly_batch_source2(time_col:Column, year:int, month:int, day:int):
    (
        spark.read
            .format("parquet")
            .load(f"/mnt/daily_batch/data_source2/{year}/{month:02d}/{day:02d}")
            .select("*", time_col.alias("ingest_time"), input_file_name().alias("source_file"))
            .withColumn("changed_date",to_date("timestamp"))
            .write
            .mode("append")
            .saveAsTable("data_source2")
    )

# COMMAND ----------

# simulation to create source file into destination
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType
from datetime import datetime

# Define the schema for Data Source 1
data_source_1_schema = StructType([
    StructField("user_id", StringType(), False),
    StructField("parcel_locker_id", StringType(), False),
    StructField("timestamp", TimestampType(), False)
])

# Create a sample DataFrame for Data Source 1
data_source_1_data = [
    ("user1", "locker2", datetime(2023, 5, 6, 12, 0, 0)),
    ("user3", "locker3", datetime(2023, 5, 6, 12, 15, 0))
]

df1 = spark.createDataFrame(data_source_1_data, schema=data_source_1_schema)

# Define the schema for Data Source 2
data_source_2_schema = StructType([
    StructField("user_id", StringType(), False),
    StructField("marketing_consent", BooleanType(), False),
    StructField("timestamp", TimestampType(), False)
])

# Create a sample DataFrame for Data Source 2
data_source_2_data = [
    ("user1", False, datetime(2023, 5, 6, 12, 5, 0)),
    ("user3", False, datetime(2023, 5, 6, 12, 10, 0))
]

df2 = spark.createDataFrame(data_source_2_data, schema=data_source_2_schema)

# Specify the destination directory based on year, month, and day
destination_path_source1 = "/mnt/daily_batch/data_source1/{}/{:02d}/{:02d}".format(
    datetime.now().year, datetime.now().month, datetime.now().day
)
destination_path_source2 = "/mnt/daily_batch/data_source2/{}/{:02d}/{:02d}".format(
    datetime.now().year, datetime.now().month, datetime.now().day
)
# Save the DataFrames as Parquet files
df1.write.parquet(destination_path_source1 )
df2.write.parquet(destination_path_source2 )

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/daily_batch/data_source1/2023/11/06/

# COMMAND ----------

c_timestamp = current_timestamp()
ingest_dailly_batch_source1(c_timestamp, 2023,11,6)

# COMMAND ----------

ingest_dailly_batch_source2(c_timestamp, 2023,11,6)

# COMMAND ----------

df1 = spark.read.table('data_source1')
display(df1)

# COMMAND ----------

df2 = spark.read.table('data_source2')
display(df2)

# COMMAND ----------

# Define a function to get the new records after daily batch
def new_records(table_name):
    return spark.read.table(table_name).filter(to_date(current_timestamp()) == to_date(col('ingest_time')))

display(new_records('data_source1'))
