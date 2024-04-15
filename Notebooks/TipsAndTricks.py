# Databricks notebook source
# MAGIC %sql
# MAGIC select * from citi_bike.fact_trips

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY citi_bike.fact_trips

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from citi_bike.fact_trips timestamp as of '2023-11-20'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE citi_bike.fact_trips_20240210 CLONE citi_bike.fact_trips

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table citi_bike.fact_trips_20240210
