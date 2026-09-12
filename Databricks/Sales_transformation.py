# Databricks notebook source
# MAGIC %run "/Users/demsvb@outlook.com/Dennis/Connectors"

# COMMAND ----------

# MAGIC %run "/Users/demsvb@outlook.com/Dennis/Generic"

# COMMAND ----------

adls_connect()

# COMMAND ----------

list_bronze_files()

# COMMAND ----------

display(
    dbutils.fs.ls(
        "abfss://dennis@dennisadlsd.dfs.core.windows.net/medallian/bronze/sales/"
    )
)

# COMMAND ----------

def read_bronze_csv(folder_name, file_name=None):
    base_path = "abfss://dennis@dennisadlsd.dfs.core.windows.net/medallian/bronze/"
    
    if file_name:
        path = f"{base_path}{folder_name}/{file_name}.csv"
    else:
        path = f"{base_path}{folder_name}/"

    return (
        spark.read
             .option("header", "true")
             .option("inferSchema", "true")
             .csv(path)
    )

# COMMAND ----------

sales_df = read_bronze_csv("sales")

display(sales_df)

# COMMAND ----------

sales_df = read_bronze_csv("sales")

# COMMAND ----------

display_data(sales_df)

# COMMAND ----------

rows_column_cnt(sales_df)

# COMMAND ----------

check_missing_values(sales_df, sales_df.columns)

# COMMAND ----------

check_duplicates(sales_df, sales_df.columns)

# COMMAND ----------

check_missing_value_percentage_v1(sales_df, sales_df.columns)

# COMMAND ----------

check_missing_values_percentage_V2(sales_df, sales_df.columns)

# COMMAND ----------

check_string_as_nan(sales_df)

# COMMAND ----------

# Transformations
from pyspark.sql.functions import split, col, to_date

sales_df = (
    sales_df
    .withColumn("Country", split(col("Location"), ";").getItem(0))
    .withColumn("Town", split(col("Location"), ";").getItem(1))

    # Convert Date to DateType
    .withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))

    # Remove unwanted columns
    .drop("Location")

    # Arrange columns
    .select(
        "ProductID",
        "SalesRepID",
        "Date",
        "Units",
        "PercentOfStandardCost",
        "RevenueDiscount",
        "Country",
        "Town"
    )
)

display(sales_df)




# COMMAND ----------

write2database(sales_df,"Sales_tb")

# COMMAND ----------

write2silver(sales_df, "Sales_S.csv")