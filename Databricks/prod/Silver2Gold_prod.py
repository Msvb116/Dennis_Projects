# Databricks notebook source
# MAGIC %run "/Users/demsvb@outlook.com/Dennis/prod/Connectors_prod"

# COMMAND ----------

# MAGIC %run "/Users/demsvb@outlook.com/Dennis/prod/Generic_prod"

# COMMAND ----------

adls_connect()

# COMMAND ----------

cate_df = read_silver_data("Category_S")
geo_df = read_silver_data("Geography_S")
pro_df = read_silver_data("Product_S")
salrep_df = read_silver_data("SalesRep_S")
sales_df = read_silver_data("Sales_S")
subcat_df = read_silver_data("SubCategory_S")

# COMMAND ----------

display_data(cate_df)


# COMMAND ----------

display_data(geo_df)

# COMMAND ----------

display_data(pro_df)

# COMMAND ----------

display_data(salrep_df)

# COMMAND ----------

display_data(sales_df)

# COMMAND ----------

display_data(subcat_df)

# COMMAND ----------

salrep_df = salrep_df.select(
    col("SalesRepID"),
    col("`Sales Rep Name`").alias("SalesRepName")
)

# COMMAND ----------

subcat_df = subcat_df.select(
    col("SubCategoryKey"),
    col("CategoryKey"),
    col("`SubCategory Name`").alias("SubCategoryName")
)

# COMMAND ----------

from pyspark.sql.functions import col

gold_df = (
    sales_df.alias("s")

    .join(
        pro_df.alias("p"),
        col("s.ProductID") == col("p.ProductID"),
        "left"
    )

    .join(
        subcat_df.alias("sc"),
        col("p.SubCategoryKey") == col("sc.SubCategoryKey"),
        "left"
    )

    .join(
        cate_df.alias("c"),
        col("sc.CategoryKey") == col("c.CategoryKey"),
        "left"
    )

    .join(
        salrep_df.alias("sr"),
        col("s.SalesRepID") == col("sr.SalesRepID"),
        "left"
    )

    .join(
        geo_df.alias("g"),
        (col("s.Country") == col("g.Country")) &
        (col("s.Town") == col("g.Town")),
        "left"
    )
)

# COMMAND ----------

gold_df = gold_df.select(

    col("s.Date"),
    col("s.ProductID"),
    col("p.ProductName"),
    col("p.Color"),
    col("p.StandardCost"),
    col("p.RetailPrice"),

    col("sc.SubCategoryName"),
    col("c.Category"),

    col("s.SalesRepID"),
    col("sr.SalesRepName"),

    col("s.Country"),
    col("s.Town"),

    col("s.Units"),
    col("s.PercentOfStandardCost"),
    col("s.RevenueDiscount")
)

# COMMAND ----------

display(gold_df)

# COMMAND ----------

gold_df = gold_df.fillna({
    "ProductName": "Unknown Product",
    "Color": "Unknown",
    "Category": "Unknown",
    "SubCategoryName": "Unknown"
})

# COMMAND ----------

gold_df = gold_df.fillna({
    "RetailPrice": 0,
    "StandardCost": 0
})

# COMMAND ----------

gold_df = gold_df.withColumn(
    "TotalRevenue",
    col("RetailPrice") * col("Units")
)

gold_df = gold_df.withColumn(
    "TotalCost",
    col("StandardCost") * col("Units")
)

gold_df = gold_df.withColumn(
    "GrossProfit",
    col("TotalRevenue") - col("TotalCost")
)

# COMMAND ----------

display(gold_df)

# COMMAND ----------

country_summary_df = (
    gold_df
    .groupBy("Country")
    .agg(
        sum("Units").alias("TotalUnits"),
        sum("TotalRevenue").alias("TotalRevenue"),
        sum("TotalCost").alias("TotalCost"),
        sum("GrossProfit").alias("GrossProfit"),
        avg("TotalRevenue").alias("AvgRevenue")
    )
)

display(country_summary_df)

# COMMAND ----------


product_summary_df = (
    gold_df
    .groupBy("ProductName","Category","SubCategoryName")
    .agg(
        sum("Units").alias("TotalUnits"),
        sum("TotalRevenue").alias("TotalRevenue"),
        sum("TotalCost").alias("TotalCost"),
        sum("GrossProfit").alias("GrossProfit")
    )
    .orderBy(col("TotalRevenue").desc())
)

display(product_summary_df)

# COMMAND ----------

salesrep_summary_df = (
    gold_df
    .groupBy("SalesRepName")
    .agg(
        sum("Units").alias("TotalUnits"),
        sum("TotalRevenue").alias("TotalRevenue"),
        sum("TotalCost").alias("TotalCost"),
        sum("GrossProfit").alias("GrossProfit")
    )
    .orderBy(col("TotalRevenue").desc())
)

display(salesrep_summary_df)

# COMMAND ----------

write2database(country_summary_df,"Countrysummary_tb")
write2database(salesrep_summary_df,"Salessummary_tb")
write2database(product_summary_df,"Productsummary_tb")

# COMMAND ----------

write2database(gold_df,"Dennis_Gold_tb")

# COMMAND ----------

write2gold(country_summary_df, "Countrysummary_G.csv")
write2gold(salesrep_summary_df, "Salessummary_G.csv")
write2gold(product_summary_df, "Productsummary_G.csv")
write2gold(gold_df, "Dennis_G.csv")