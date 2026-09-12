# Databricks notebook source
# MAGIC %run "/Users/demsvb@outlook.com/Dennis/Connectors"

# COMMAND ----------

# MAGIC %run "/Users/demsvb@outlook.com/Dennis/Generic"

# COMMAND ----------

adls_connect()

# COMMAND ----------

list_bronze_files()

# COMMAND ----------

prd_df = read_bronze_csv("Product")

# COMMAND ----------

display_data(prd_df)

# COMMAND ----------

display(prd_df)

# COMMAND ----------

rows_column_cnt(prd_df)

# COMMAND ----------

check_missing_values(prd_df, prd_df.columns)

# COMMAND ----------

check_duplicates(prd_df, prd_df.columns)

# COMMAND ----------

check_missing_value_percentage_v1(prd_df, prd_df.columns)

# COMMAND ----------

check_missing_values_percentage_V2(prd_df, prd_df.columns)

# COMMAND ----------

check_string_as_nan(prd_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("Sub Category Key").orderBy("ProductID")
prd_df = prd_df.withColumn(
    "RN",
    row_number().over(window_spec)
)

# COMMAND ----------

display_data(prd_df)

# COMMAND ----------

from pyspark.sql.functions import col

prd_df = prd_df.filter(col("RN") == 1)

# COMMAND ----------

prd_df = prd_df.drop("RN")

# COMMAND ----------

prd_df = prd_df.withColumnRenamed("Sub Category Key", "SubCategoryKey")

# COMMAND ----------

print(prd_df.columns)

# COMMAND ----------

write2database(prd_df,"Product_tb")

# COMMAND ----------

write2silver(prd_df, "Product_S.csv")