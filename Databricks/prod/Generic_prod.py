# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.functions import *

# COMMAND ----------

def display_data(df):
    return display(df.limit(5))

# COMMAND ----------

def rows_column_cnt(df):
    return df.count(), len(df.columns)
    

# COMMAND ----------

def check_missing_values(df, lst_cln):
    missing_values = {}
    for i in lst_cln:
        a = df.filter(col(i).isNull()).count()
        missing_values[i] = a
    return missing_values    


# COMMAND ----------

def check_duplicates(df, cln):
    a = df.select(cln).distinct().count()
    b = df.select(cln).count()
    if a == b:
        print("No Duplicates")
    else:
        c = b - a
        print("There are ",c,"dupliucates")     

# COMMAND ----------

def check_missing_value_percentage_v1(df, lst_cl):
    missing_value = {}

    for i in lst_cl:
        a = df.filter(col(i).isNull()).count()
        missing_value[i] = a

    return missing_value

# COMMAND ----------

def check_missing_values_percentage_V2(df, last_cl):
    global missing_values_percent_less_than_75
    global missing_values_percent_more_than_75
    missing_values_percent_less_than_75 = {}
    missing_values_percent_more_than_75 = {}
    b = df.count()
    for i in last_cl:
        a = df.filter(col(i).isNull()).count()
        c = (a/b)*100
        if c > 75:
            missing_values_percent_more_than_75[i] = c
        else:
            missing_values_percent_less_than_75[i] = c
    return {"missing_values_percent_more_than_75": missing_values_percent_more_than_75,
                   "missing_values_percent_less_than_75": missing_values_percent_less_than_75 }  

# COMMAND ----------

def dropcolumns(df, col_lst):
    for i in col_lst:
        df = df.drop(i)
        print("Dropped Columns:",i)
    return df   

# COMMAND ----------

def check_string_as_nan(df):
    results = {}
    for i in df.columns:
        results[i] = df.filter(col(i).like("NaN")).count()
    return results   

# COMMAND ----------

def mean_impute(df, column):
    value = df.select(mean(column)).first()[0]
    return df.fillna({column: value})

# COMMAND ----------

def median_impute(df, column):
    value = df.approxQuantile(column, [0.5], 0.01)[0]
    return df.fillna({column: value})

# COMMAND ----------

def mode_impute(df, column):
    value = df.groupBy(column).count() \
              .orderBy(col("count").desc()) \
              .first()[0]

    return df.fillna({column: value})

# COMMAND ----------

def masking_phone(cl):
    masked_phone = cl[:3] + "*****" + cl[-2:]
    return masked_phone