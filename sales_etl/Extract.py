# Databricks notebook source
# Import necessary libraries
from pyspark.sql import SparkSession

# COMMAND ----------

sales_df = spark.table("store_server.default.supermarket_sales")
display(sales_df)

# COMMAND ----------

# Extract batch data from the sales_df DataFrame
batch_data_df = sales_df

# Store the raw batch data temporarily in a temporary view
batch_data_df.createOrReplaceTempView("temp_raw_batch_data")

# Display the temporary view to verify
display(spark.sql("SELECT * FROM temp_raw_batch_data"))
