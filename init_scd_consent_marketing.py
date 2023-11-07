# Databricks notebook source
query="""
create or replace table scd_consent_marketing 
as 
select 
  user_id,
  marketing_consent,
  timestamp as start_timestamp,
  LEAD(timestamp,1) over (
    partition by user_id
    order by timestamp
  ) end_timestamp,
  case when end_timestamp is null then 'current' else '' end as flag 
from data_source2 ;
"""

# COMMAND ----------

spark.sql(query)
