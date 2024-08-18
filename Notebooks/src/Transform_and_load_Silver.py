# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Import libraries for transforms
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import to_date
from pyspark.sql.functions import col, trim, lower
from pyspark.sql.functions import monotonically_increasing_id

# COMMAND ----------

df_start_stations = spark.sql(
    "SELECT start_station_id as Station_ID, start_station_name as Station_Name, start_station_latitude as Station_Latitude, start_station_longitude as Station_Longitude FROM citi_bike_dev.bronze.citi_bike_data"
)

df_end_stations = spark.sql(
    "SELECT end_station_id as Station_ID, end_station_name as Station_Name, end_station_latitude as Station_Latitude, end_station_longitude as Station_Longitude FROM citi_bike_dev.bronze.citi_bike_data"
)

df_stations = df_start_stations.union(df_end_stations)

df_stations = df_stations.distinct()

# Add an key column to each dataframe to act as a Surrogate Key.
df_stations = df_stations.withColumn("Stations_key", monotonically_increasing_id())

# COMMAND ----------

df_bikes = spark.sql("SELECT bike_id from citi_bike_dev.bronze.citi_bike_data")

df_bikes = df_bikes.distinct()

# Add an key column to each dataframe to act as a Surrogate Key.
df_bikes = df_bikes.withColumn("Bike_key", monotonically_increasing_id())

# COMMAND ----------

silver_delta_location = "citi_bike_dev.silver"

spark.sql(f"DROP TABLE IF EXISTS {silver_delta_location}.dim_stations")

df_stations.write.format("delta").mode("overwrite").saveAsTable(
    silver_delta_location + ".dim_stations"
)

spark.sql(f"DROP TABLE IF EXISTS {silver_delta_location}.dim_bikes")

df_bikes.write.format("delta").mode("overwrite").saveAsTable(
    silver_delta_location + ".dim_bikes"
)

spark.sql(f"DROP TABLE IF EXISTS {silver_delta_location}.dim_date")

df_dates.write.format("delta").mode("overwrite").saveAsTable(
    silver_delta_location + ".dim_date"
)

# COMMAND ----------

df_trips = spark.sql(""" 
     SELECT  d.date as Rental_Date_Key, 
             bd.trip_duration, 
             coalesce(s.stations_key, -1) as Start_Station_Key, 
             coalesce(es.stations_key, -1) as End_Station_Key, 
             coalesce(b.bike_key, -1) as Bike_Key  
     FROM citi_bike_dev.bronze.citi_bike_data as bd 
     LEFT JOIN citi_bike_dev.silver.dim_date d ON bd.rental_date = d.Date 
     LEFT JOIN citi_bike_dev.silver.dim_bikes b ON bd.bike_id = b.bike_id 
     LEFT JOIN citi_bike_dev.silver.dim_stations s on bd.start_station_id = s.station_id 
     LEFT JOIN citi_bike_dev.silver.dim_stations es on bd.end_station_id = es.station_id 
""")

# COMMAND ----------

display(df_trips)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select * from citi_bike_dev.bronze.citi_bike_data

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {silver_delta_location}.fact_trips")

df_trips.write.format("delta").mode("overwrite").saveAsTable(
    silver_location + ".fact_trips"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from citi_bike_dev.silver.fact_trips
