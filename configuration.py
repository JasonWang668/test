# Databricks notebook source
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.join.enabled", "true")

# COMMAND ----------

df_source1 = spark.table('data_source1')
df_source2 = spark.table('data_source2')
