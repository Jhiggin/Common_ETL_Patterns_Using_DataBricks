# Databricks notebook source
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    TimestampType,
    DoubleType,
)

from pyspark.sql.functions import (
    col, 
    lit, 
    to_date, 
    date_format, 
    year, 
    month, 
    dayofmonth, 
    quarter, 
    dayofweek
)

from datetime import (
    datetime, 
    timedelta
)

# COMMAND ----------

# set the data lake file location:
file_location = ("/mnt/data/in/*.csv")

# COMMAND ----------

# Define schema
schema = StructType(
    [
        StructField("trip_duration", IntegerType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("stop_time", TimestampType(), True),
        StructField("start_station_id", IntegerType(), True),
        StructField("start_station_name", StringType(), True),
        StructField("start_station_latitude", DoubleType(), True),
        StructField("start_station_longitude", DoubleType(), True),
        StructField("end_station_id", IntegerType(), True),
        StructField("end_station_name", StringType(), True),
        StructField("end_station_latitude", DoubleType(), True),
        StructField("end_station_longitude", DoubleType(), True),
        StructField("bike_id", IntegerType(), True),
        StructField("user_type", StringType(), True),
        StructField("birth_year", IntegerType(), True),
        StructField("gender", IntegerType(), True),
    ]
)

# load the data from the csv file to a data frame
df_citi_bike_data = (
    spark.read.option("header", "true")
    .schema(schema)
    .option("delimiter", ",")
    .csv(file_location)
)

# COMMAND ----------

df_citi_bike_data = df_citi_bike_data.withColumn('Rental_Date', to_date('start_time'))

# COMMAND ----------

bronze_delta_location = "citi_bike_dev.bronze"

df_citi_bike_data.write.format("delta").mode("overwrite").saveAsTable(
    bronze_delta_location + ".citi_bike_data"
)

# COMMAND ----------

def create_date_table(start_date, end_date):
    """
    Create a date dimension table.
    
    :param start_date: string, start date in 'YYYY-MM-DD' format
    :param end_date: string, end date in 'YYYY-MM-DD' format
    :return: DataFrame of the date dimension
    """

    # Generate a range of dates
    total_days = (datetime.strptime(end_date, '%Y-%m-%d') - datetime.strptime(start_date, '%Y-%m-%d')).days
    date_list = [(datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=x)).date() for x in range(total_days + 1)]
    dates_df = spark.createDataFrame(date_list, 'date')

    # Expand the date information
    date_table = dates_df.select(
        col('value').alias('date'),
        year('date').alias('year'),
        quarter('date').alias('quarter'),
        month('date').alias('month'),
        dayofmonth('date').alias('day_of_month'),
        dayofweek('date').alias('day_of_week'),
        date_format('date', 'E').alias('weekday'),
        (dayofweek('date') > 5).cast('boolean').alias('is_weekend')
    )

    return date_table

# COMMAND ----------

start_date = '2005-01-01'
end_date = '2030-12-31'
date_table_df = create_date_table(start_date, end_date)

# COMMAND ----------

date_table_df.write.format("delta").mode("overwrite").saveAsTable(
    bronze_location + ".adventureworksdw_dim_date"
)
