# Databricks notebook source
# MAGIC %md
# MAGIC <table border="0">
# MAGIC     <tr>
# MAGIC         <th>File Format</th>
# MAGIC         <th>overwriteSchema</th>
# MAGIC         <th>mergeSchema</th>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC         <td>Parquet</td>
# MAGIC         <td>✅ Yes</td>
# MAGIC         <td>✅ Yes</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC         <td>ORC</td>
# MAGIC         <td>✅ Yes</td>
# MAGIC         <td>✅ Yes</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC         <td>Delta</td>
# MAGIC         <td>✅ Yes</td>
# MAGIC         <td>❌ No</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC         <td>JSON</td>
# MAGIC         <td>❌ No</td>
# MAGIC         <td>❌ No</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC         <td>CSV</td>
# MAGIC         <td>❌ No</td>
# MAGIC         <td>❌ No</td>
# MAGIC     </tr>
# MAGIC     <tr>
# MAGIC         <td>Avro</td>
# MAGIC         <td>❌ No</td>
# MAGIC         <td>❌ No</td>
# MAGIC     </tr>
# MAGIC </table>
# MAGIC has context menu

# COMMAND ----------

# MAGIC %md
# MAGIC ### merge schema

# COMMAND ----------

from pyspark.sql import SparkSession

sales_data_1 = [(101, "Laptop", 1500), (102, "Smartphone", 700), (103, "Tablet", 450)]
columns_1 = ["product_id", "product_name", "sales_amount"]
df_sales_1 = spark.createDataFrame(sales_data_1, columns_1)

df_sales_1.write.mode("overwrite").parquet("abfss://source@internsstorage1.dfs.core.windows.net/anusha/mergeschema_1.parquet")

# COMMAND ----------

display(df_sales_1)

# COMMAND ----------

sales_data_2 = [
    (104, "Monitor", 300, "North America", "2025-01-15"),
    (105, "Headphones", 120, "Europe", "2025-01-16")
]
columns_2 = ["product_id", "product_name", "sales_amount", "sales_region", "sales_date"]
df_sales_2 = spark.createDataFrame(sales_data_2, columns_2)

# COMMAND ----------

display(df_sales_2)

# COMMAND ----------

df_sales_2.write.option("mergeSchema", "true").mode("append").parquet("abfss://source@internsstorage1.dfs.core.windows.net/anusha/mergeschema_1.parquet")

# COMMAND ----------

merged_sales_df = spark.read.parquet("abfss://source@internsstorage1.dfs.core.windows.net/anusha/mergeschema_1.parquet")


merged_sales_df.printSchema()
merged_sales_df.show()

# COMMAND ----------

sales_data_3 = [
    (106, "Monitor", "North America", "2025-01-15"),
    (107, "Headphones", "Europe", "2025-01-16")
]
columns_3 = ["product_id", "product_name", "sales_region", "sales_date"]#sales amount
df_sales_3 = spark.createDataFrame(sales_data_3, columns_3)

# COMMAND ----------

df_sales_3.write.option("mergeSchema", "true").mode("append").parquet("abfss://source@internsstorage1.dfs.core.windows.net/anusha/mergeschema_1.parquet")

# COMMAND ----------

merged_sales_df = spark.read.parquet("abfss://source@internsstorage1.dfs.core.windows.net/anusha/mergeschema_1.parquet")


merged_sales_df.printSchema()
merged_sales_df.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### overwrite schema

# COMMAND ----------


display(df_sales_1)

# COMMAND ----------

display(df_sales_2)

# COMMAND ----------

df_sales_2.write.option("overwriteSchema", "true").mode("overwrite").parquet("abfss://source@internsstorage1.dfs.core.windows.net/anusha/mergeschema_1.parquet")


# COMMAND ----------


overwritten_sales_df = spark.read.parquet("abfss://source@internsstorage1.dfs.core.windows.net/anusha/mergeschema_1.parquet")


overwritten_sales_df.printSchema()
overwritten_sales_df.show()




# COMMAND ----------

# MAGIC %md
# MAGIC ### with delta merge and overwrite schema

# COMMAND ----------

display(df_sales_1)


# COMMAND ----------

display(df_sales_2)

# COMMAND ----------

df_sales_1.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("abfss://source@internsstorage1.dfs.core.windows.net/anusha/overwrite.delta")

# COMMAND ----------

df_sales_2.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("abfss://source@internsstorage1.dfs.core.windows.net/anusha/overwrite.delta")


# COMMAND ----------

overwritten_sales_df = spark.read.format("delta").load(
    "abfss://source@internsstorage1.dfs.core.windows.net/anusha/overwrite.delta"
)

overwritten_sales_df.printSchema()
display(overwritten_sales_df)

# COMMAND ----------

merged_df=df_sales_1.write.format("delta").option("mergeSchema","true").mode("append").save("abfss://source@internsstorage1.dfs.core.windows.net/anusha/merge.delta")

# COMMAND ----------

display(merged_df)

# COMMAND ----------

merged_df=df_sales_2.write.format("delta").option("mergeSchema","true").mode("append").save("abfss://source@internsstorage1.dfs.core.windows.net/anusha/merge.delta")

#merged_df.show()
type(merged_df)

# COMMAND ----------

merge_sales_df = spark.read.format("delta").load(
    "abfss://source@internsstorage1.dfs.core.windows.net/anusha/merge.delta"
)

merged_sales_df.printSchema()
display(merged_sales_df)