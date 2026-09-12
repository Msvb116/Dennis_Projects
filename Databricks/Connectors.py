# Databricks notebook source
def adls_connect():
    spark.conf.set(
    "fs.azure.account.key.dennisadlsd.dfs.core.windows.net",
    dbutils.secrets.get(scope="dennisdbxscope", key="adlspasskey"))
    return "ADLS Connected"

# COMMAND ----------

def list_bronze_files():
    display(
        dbutils.fs.ls(
            "abfss://dennis@dennisadlsd.dfs.core.windows.net/medallian/bronze/"
        )
    )
    return "Bronze Files Listed"    

# COMMAND ----------

def list_silver_files():
    display(
        dbutils.fs.ls(
            "abfss://dennis@dennisadlsd.dfs.core.windows.net/medallian/silver/"
        )
    )
    return "silver Files Listed"      

# COMMAND ----------

def list_gold_files():
    display(
        dbutils.fs.ls(
            "abfss://dennis@dennisadlsd.dfs.core.windows.net/medallian/gold/"
        )
    )
    return "Gold Files Listed"     

# COMMAND ----------

def read_bronze_csv(file_name):
    data = spark.read.csv("abfss://dennis@dennisadlsd.dfs.core.windows.net/medallian/bronze/"+file_name+".csv", inferSchema = True , header = True)
    return data

# COMMAND ----------

#def read_bronze_json(file_name):
#    data = spark.read.json("abfss://dennis@dennisadlsd.dfs.core.windows.net/medallian/bronze/"+file_name+".json")
#    return data

def read_bronze_json(file_name):
    data = (spark.read.option("multiline", "true").json(f"abfss://dennis@dennisadlsd.dfs.core.windows.net/medallian/bronze/{file_name}.json"))
    return data    

# COMMAND ----------

def read_silver_data(file_name):
    data = spark.read.csv("abfss://dennis@dennisadlsd.dfs.core.windows.net/medallian/silver/"+file_name+".csv", inferSchema = True , header = True)
    return data

# COMMAND ----------

def read_silver_json(file_name):
    data = spark.read.json("abfss://dennis@dennisadlsd.dfs.core.windows.net/medallian/silver/"+file_name+".json")
    return data

# COMMAND ----------

def read_gold_data(file_name):
    data = spark.read.csv("abfss://dennis@dennisadlsd.dfs.core.windows.net/medallian/gold/"+file_name+".csv", inferSchema = True , header = True)
    return data

# COMMAND ----------

def write2database(df ,table_name):
    HostName = dbutils.secrets.get(scope="dennisdbxscope", key ="asqlserver")
    PortNO = dbutils.secrets.get(scope="dennisdbxscope", key = "asqlportno")
    DatabaseName = dbutils.secrets.get(scope="dennisdbxscope", key = "azuredbname")
    DBProperties = {
    "user":dbutils.secrets.get(scope="dennisdbxscope", key ="azureuser"),
    "password":dbutils.secrets.get(scope="dennisdbxscope", key ="adbpassword")}
    urloftgt = "jdbc:sqlserver://{0}:{1};database={2}".format(HostName,PortNO,DatabaseName)
    output = df.write.jdbc(url = urloftgt, table = table_name, mode = "overwrite", properties=DBProperties)
    print("Data written to table "+table_name)
    



# COMMAND ----------

def write2silver(df, file_name):
    silverpath = "abfss://dennis@dennisadlsd.dfs.core.windows.net/medallian/silver"
    temp_path = f"{silverpath}/temp"
    final_path = f"{silverpath}/{file_name}"
    df.write.mode("overwrite") \
        .option("header", "true") \
        .csv(temp_path)
    files = dbutils.fs.ls(temp_path)
    csv_file = [
        file.path for file in files
        if file.path.endswith(".csv")][0]
    dbutils.fs.mv(csv_file, final_path)
    dbutils.fs.rm(temp_path, recurse=True)

    print("*********Successfully Written in SilverLayer**************")

# COMMAND ----------

def write2gold(df, file_name):
    goldpath = "abfss://dennis@dennisadlsd.dfs.core.windows.net/medallian/gold"
    temp_path = f"{goldpath}/temp"
    final_path = f"{goldpath}/{file_name}"
    df.write.mode("overwrite") \
        .option("header", "true") \
        .csv(temp_path)
    files = dbutils.fs.ls(temp_path)
    csv_file = [
        file.path for file in files
        if file.path.endswith(".csv")][0]
    dbutils.fs.mv(csv_file, final_path)
    dbutils.fs.rm(temp_path, recurse=True)

    print("*********Successfully Written in goldLayer**************")