# Databricks notebook source
# MAGIC %run "/Users/demsvb@outlook.com/Dennis/Connectors"

# COMMAND ----------

# MAGIC %run "/Users/demsvb@outlook.com/Dennis/Generic"

# COMMAND ----------

adls_connect()

# COMMAND ----------

list_bronze_files()

# COMMAND ----------

ctg_df = read_bronze_csv("Categories")

# COMMAND ----------

display_data(ctg_df)

# COMMAND ----------

rows_column_cnt(ctg_df)

# COMMAND ----------

check_missing_values(ctg_df, ctg_df.columns)

# COMMAND ----------

check_duplicates(ctg_df, ctg_df.columns)

# COMMAND ----------

check_missing_value_percentage_v1(ctg_df, ctg_df.columns)

# COMMAND ----------

check_missing_values_percentage_V2(ctg_df, ctg_df.columns)

# COMMAND ----------

check_string_as_nan(ctg_df)

# COMMAND ----------

ctg_df = ctg_df.replace("NaN",None)

# COMMAND ----------

write2database(ctg_df,"Category_tb")

# COMMAND ----------

write2silver(ctg_df, "Category_S.csv")