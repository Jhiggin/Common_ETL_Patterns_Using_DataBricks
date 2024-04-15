# Databricks notebook source
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    TimestampType,
    DoubleType,
)

# Function to read YAML configuration from DBFS
def read_config_from_dbfs(dbfs_path):
    with open(dbfs_path, 'r') as file:
        return yaml.safe_load(file)

# Path to your YAML configuration in DBFS
config_path = '/Workspace/Repos/joshua.higginbotham@codenamesql.com/Common_ETL_Patterns_Using_DataBricks/configs/client_configs.yaml'
configs = read_config_from_dbfs(config_path)

# COMMAND ----------

# Function to parse the schema from YAML configuration
def parse_schema(schema_config):
    type_mapping = {
        "IntegerType": IntegerType(),
        "StringType": StringType(),
        "DoubleType": DoubleType(),
        "TimestampType": TimestampType()
    }
    fields = [StructField(field['name'], type_mapping[field['type']], True) for field in schema_config]
    return StructType(fields)

# COMMAND ----------

# Extract client1 configuration and schema
client1_config = configs['client1']
schema = parse_schema(client1_config['schema'])

# COMMAND ----------

# Function to apply transformations
def transform_data(df, transformations):
    for transformation in transformations:
        if transformation['type'] == 'filter':
            df = df.filter(transformation['expression'])
        elif transformation['type'] == 'withColumn':
            df = df.withColumn(transformation['column'], expr(transformation['expression']))
        # Extend with more transformation types as needed
    return df

# Function to process each client's data
def process_client_data(client_id, client_config):
    df = spark.read.format(client_config['format']).schema(schema).options(**client1_config['options']).load(client_config['path'])
    display(df)
    transformed_df = transform_data(df, client_config['transformations'])
    transformed_df.write.mode('overwrite').format(client_config['output_format']).save(client_config['output_path'])
    print(f"Processed data for client {client_id}")

# COMMAND ----------

process_client_data("client1", client1_config)

# COMMAND ----------

df = spark.read.parquet("/mnt/data/out/output/client_1_data*")

# COMMAND ----------

display(df)
