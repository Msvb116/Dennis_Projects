# Databricks notebook source
# MAGIC %run "/Users/demsvb@outlook.com/Dennis/prod/Connectors_prod"

# COMMAND ----------

# MAGIC %run "/Users/demsvb@outlook.com/Dennis/prod/Generic_prod"

# COMMAND ----------

adls_connect()

# COMMAND ----------

salesrep_df = read_bronze_csv("SalesRep")

# COMMAND ----------

rows_column_cnt(salesrep_df)

# COMMAND ----------

check_missing_values(salesrep_df, salesrep_df.columns)

# COMMAND ----------

check_duplicates(salesrep_df, salesrep_df.columns)

# COMMAND ----------

check_missing_value_percentage_v1(salesrep_df, salesrep_df.columns)

# COMMAND ----------

check_missing_values_percentage_V2(salesrep_df, salesrep_df.columns)

# COMMAND ----------

check_string_as_nan(salesrep_df)

# COMMAND ----------

salesrep_df = salesrep_df.withColumn("SalesRepID",regexp_replace(col("SalesRepID"), "[^0-9]", ""))
salesrep_df = salesrep_df.withColumn("SalesRepID",col("SalesRepID").cast("int"))


# COMMAND ----------

write2database(salesrep_df,"SalesRep_tb")

# COMMAND ----------

write2silver(salesrep_df, "SalesRep_S.csv")