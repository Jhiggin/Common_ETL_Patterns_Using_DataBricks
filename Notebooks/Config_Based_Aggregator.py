# Databricks notebook source
import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

class DataProcessor:
    def __init__(self, tables_config_path, metrics_config_path):
        self.spark = SparkSession.builder.appName("DataProcessor").getOrCreate()
        with open(tables_config_path, 'r') as file:
            self.tables_config = yaml.safe_load(file)
        with open(metrics_config_path, 'r') as file:
            self.metrics_config = yaml.safe_load(file)
        self.df = None
    
    def load_data(self, tables_to_join):
        # Load core table
        core_config = self.tables_config['tables']['trips']
        self.df = self.spark.sql(f"SELECT * FROM {core_config['table_path']}")
        
        # Load and join additional tables
        for table in tables_to_join:
            if table in self.tables_config['tables']:
                table_config = self.tables_config['tables'][table]
                table_df = self.spark.sql(f"SELECT * FROM {table_config['table_path']}")
                join_key = table_config['join_key']
                self.df = self.df.join(table_df, join_key, 'left')
    
    def filter_by_date(self, start_date, end_date):
        self.df = self.df.filter((F.col("Rental_Date_Key") >= start_date) & (F.col("Rental_Date_Key") <= end_date))
    
    def group_and_calculate_metrics(self, group_by_columns, metrics):
        if group_by_columns:
            grouped_df = self.df.groupBy(group_by_columns)
        else:
            grouped_df = self.df
        
        agg_expressions = []
        for metric_name in metrics:
            metric_info = self.metrics_config['metrics'].get(metric_name)
            if metric_info:
                query = metric_info['query']
                display_name = metric_info['display_name']
                agg_expressions.append(F.expr(query).alias(display_name))
        
        result_df = grouped_df.agg(*agg_expressions)
        return result_df

def get_metrics(start_date, end_date, group_by, metrics, tables_to_join):
    processor = DataProcessor("/Workspace/Repos/joshua.higginbotham@codenamesql.com/Common_ETL_Patterns_Using_DataBricks/configs/table_config.yaml", "/Workspace/Repos/joshua.higginbotham@codenamesql.com/Common_ETL_Patterns_Using_DataBricks/configs/metrics_config.yaml")
    processor.load_data(tables_to_join)
    processor.filter_by_date(start_date, end_date)
    result_df = processor.group_and_calculate_metrics(group_by, metrics)
    return result_df


# COMMAND ----------

# Usage Example
metrics = get_metrics(
    start_date="2021-01-01",
    end_date="2021-01-07",
    group_by=["bike_id"],
    metrics=["Trip_Count"],
    tables_to_join=["bikes"]
)

# COMMAND ----------

metrics.show()
