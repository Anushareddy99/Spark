# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Managed Table Example") \
    .getOrCreate()


data = [("James", "Sales", 3000),
        ("Michael", "Sales", 4600),
        ("Robert", "Engineering", 4100),
        ("Maria", "Finance", 3000)]


columns = ["Employee_Name", "Department", "Salary"]


df = spark.createDataFrame(data, columns)

df.write \
    .format("delta") \
    .saveAsTable("managed_employees")


display(spark.sql("SHOW TABLES"))

display(spark.sql("SELECT * FROM managed_employees"))

#spark.sql("DROP TABLE managed_employees")

# COMMAND ----------

# MAGIC %fs ls abfss://source@internsstorage1.dfs.core.windows.net/anusha/

# COMMAND ----------

external_path = "abfss://source@internsstorage1.dfs.core.windows.net/anusha/"


df_external = spark.createDataFrame(data, columns)

df_external.write \
    .format("parquet") \
    .option("path", "abfss://source@internsstorage1.dfs.core.windows.net/anusha/external_employees") \
    .mode("overwrite") \
    .saveAsTable("interns_adls.anusha.external_employees")


display(spark.sql("SHOW TABLES"))