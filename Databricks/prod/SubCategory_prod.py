# Databricks notebook source
# MAGIC %run "/Users/demsvb@outlook.com/Dennis/prod/Connectors_prod"

# COMMAND ----------

# MAGIC %run "/Users/demsvb@outlook.com/Dennis/prod/Generic_prod"

# COMMAND ----------

adls_connect()

# COMMAND ----------

subc_df = read_bronze_csv("SubCategories")

# COMMAND ----------

rows_column_cnt(subc_df)

# COMMAND ----------

check_missing_values(subc_df, subc_df.columns)

# COMMAND ----------

check_duplicates(subc_df, subc_df.columns)

# COMMAND ----------

check_missing_value_percentage_v1(subc_df, subc_df.columns)

# COMMAND ----------

check_missing_values_percentage_V2(subc_df, subc_df.columns)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

subc_df = subc_df.withColumn(
    "CategoryKey",
    regexp_replace(col("CategoryKey"), r"ID\s*-\s*", "")
)

display(subc_df)

# COMMAND ----------

check_string_as_nan(subc_df)

# COMMAND ----------

write2database(subc_df,"SubCategory_tb")

# COMMAND ----------

write2silver(subc_df, "SubCategory_S.csv")