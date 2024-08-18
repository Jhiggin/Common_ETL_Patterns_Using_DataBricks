# Databricks notebook source
import yaml
from pyspark.sql import functions as F

class DataProcessor:
    def __init__(self, tables_config_path, metrics_config_path):
        with open(tables_config_path, 'r') as file:
            self.tables_config = yaml.safe_load(file)
        with open(metrics_config_path, 'r') as file:
            self.metrics_config = yaml.safe_load(file)
        self.df = None
    
    def load_data(self, tables_to_join):
        # Load core table
        core_config = self.tables_config['tables']['trips']
        self.df = spark.sql(f"SELECT * FROM {core_config['table_path']}")
        
        # Load and join additional tables
        for table in tables_to_join:
            if table in self.tables_config['tables']:
                table_config = self.tables_config['tables'][table]
                table_df = spark.sql(f"SELECT * FROM {table_config['table_path']}")
                join_key = table_config['join_key']
                self.df = self.df.join(table_df, join_key, 'left')
    
    def filter_by_date(self, start_date, end_date):
        self.df = self.df.filter(
            (F.col("Rental_Date_Key") >= start_date) & 
            (F.col("Rental_Date_Key") <= end_date)
        )
    
    def group_and_calculate_metrics(self, group_by_columns, metrics):
        grouped_df = self.df.groupBy(group_by_columns) if group_by_columns else self.df
        
        agg_expressions = [
            F.expr(self.metrics_config['metrics'][metric_name]['query']).alias(self.metrics_config['metrics'][metric_name]['display_name'])
            for metric_name in metrics if metric_name in self.metrics_config['metrics']
        ]
        
        result_df = grouped_df.agg(*agg_expressions)
        return result_df

def get_metrics(start_date, end_date, group_by, metrics, tables_to_join, tables_config_path, metrics_config_path):
    processor = DataProcessor(tables_config_path, metrics_config_path)
    processor.load_data(tables_to_join)
    processor.filter_by_date(start_date, end_date)
    result_df = processor.group_and_calculate_metrics(group_by, metrics)
    return result_df


# COMMAND ----------

# Usage Example
tables_config_path = "/Workspace/Repos/joshua.higginbotham@codenamesql.com/Common_ETL_Patterns_Using_DataBricks/configs/table_config.yaml"
metrics_config_path = "/Workspace/Repos/joshua.higginbotham@codenamesql.com/Common_ETL_Patterns_Using_DataBricks/configs/metrics_config.yaml"

metrics = get_metrics(
    start_date="2021-01-01",
    end_date="2021-01-07",
    group_by=["bike_id"],
    metrics=["Trip_Count", "Avg_Trip_Duration"],
    tables_to_join=["bikes"],
    tables_config_path=tables_config_path,
    metrics_config_path=metrics_config_path
)

# COMMAND ----------

display(metrics)
