# Databricks notebook source
# MAGIC %run "/Users/demsvb@outlook.com/Dennis/Connectors"

# COMMAND ----------

# MAGIC %run "/Users/demsvb@outlook.com/Dennis/Generic"

# COMMAND ----------

adls_connect()

# COMMAND ----------

list_bronze_files()

# COMMAND ----------

geo_df = read_bronze_json("Geography")

# COMMAND ----------

display_data(geo_df)

# COMMAND ----------

def read_bronze_json(file_name):
    data = (
        spark.read
             .option("multiline", "true")
             .json(
                 f"abfss://dennis@dennisadlsd.dfs.core.windows.net/medallian/bronze/{file_name}.json"
             )
    )
    return data

# COMMAND ----------

geo_df = read_bronze_json("Geography")

# COMMAND ----------

geo_df.printSchema()

# COMMAND ----------

rows_column_cnt(geo_df)

# COMMAND ----------

geo_df = geo_df.drop("Wikipedia")

# COMMAND ----------

check_missing_values(geo_df, geo_df.columns)

# COMMAND ----------

check_duplicates(geo_df, geo_df.columns)

# COMMAND ----------

check_missing_value_percentage_v1(geo_df, geo_df.columns)

# COMMAND ----------

check_missing_values_percentage_V2(geo_df, geo_df.columns)

# COMMAND ----------

check_string_as_nan(geo_df)

# COMMAND ----------

write2database(geo_df,"Geograpphy_tb")

# COMMAND ----------

write2silver(geo_df, "Geography_S.csv")