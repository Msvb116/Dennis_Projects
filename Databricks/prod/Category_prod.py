# Databricks notebook source
# MAGIC %run "/Users/demsvb@outlook.com/Dennis/prod/Connectors_prod"

# COMMAND ----------

# MAGIC %run "/Users/demsvb@outlook.com/Dennis/prod/Generic_prod"

# COMMAND ----------

adls_connect()

# COMMAND ----------

ctg_df = read_bronze_csv("Categories")

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