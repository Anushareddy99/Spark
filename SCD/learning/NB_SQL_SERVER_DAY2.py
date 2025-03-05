# Databricks notebook source
# MAGIC %md
# MAGIC CREATE TABLE Employee_2(
# MAGIC     ID INT PRIMARY KEY,
# MAGIC     Name VARCHAR(100),
# MAGIC     Age INT,
# MAGIC     Department VARCHAR(100)
# MAGIC );

# COMMAND ----------


INSERT INTO Employee_2(ID, Name, Age, Department)
VALUES 
(1, 'Alice Smith', 30, 'HR'),
(2, 'Bob Johnson', 35, 'Finance'),
(3, 'Charlie Brown', 28, 'IT'),
(4, 'David Williams', 40, 'Marketing'),
(5, 'Eve Davis', 25, 'Sales'),
(6, 'Frank Miller', 45, 'Finance'),
(7, 'Grace Lee', 32, 'HR'),
(8, 'Henry Wilson', 38, 'IT'),
(9, 'Ivy Moore', 29, 'Sales'),
(10, 'Jack Taylor', 34, 'HR'),
(11, 'Karen Anderson', 41, 'Marketing'),
(12, 'Leo Thomas', 27, 'Finance'),
(13, 'Mona Jackson', 33, 'Sales'),
(14, 'Nancy White', 36, 'IT'),
(15, 'Oscar Harris', 39, 'Finance'),
(16, 'Paul Clark', 28, 'HR'),
(17, 'Quinn Rodriguez', 30, 'Marketing'),
(18, 'Rita Lewis', 26, 'Sales'),
(19, 'Steve Walker', 31, 'IT'),
(20, 'Tina Hall', 37, 'HR'),
(21, 'Uma Young', 33, 'Finance'),
(22, 'Vera King', 29, 'Sales'),
(23, 'Will Scott', 40, 'IT'),
(24, 'Xander Hill', 34, 'HR'),
(25, 'Yara Adams', 27, 'Marketing');


# COMMAND ----------

# MAGIC %md
# MAGIC 2.read this sql table in databricks and create dataframe

# COMMAND ----------

jdbc_url = "jdbc:sqlserver://azure-intern-sqlserver.database.windows.net:1433;database=azure-sql-intern-db-01"
properties = {
    "user": "intern",
    "password": "Syren@123",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

df_sql = spark.read.jdbc(jdbc_url, "Employee_2", properties=properties)

df_sql.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 3.perform any transformation in pyspark

# COMMAND ----------

df_transformed = df_sql.filter(df_sql.Age > 25)

df_transformed.show()

# COMMAND ----------

from pyspark.sql.functions import split

# COMMAND ----------

df_split = df_transformed.withColumn("First_Name", split(df_transformed["Name"], " ").getItem(0)) \
             .withColumn("Last_Name", split(df_transformed["Name"], " ").getItem(1))
display(df_split)

# COMMAND ----------

from pyspark.sql.functions import concat_ws, lower,lit

# COMMAND ----------

df_with_email = df_split.withColumn(
    "email", 
    concat_ws(".", lower(df_split.First_Name), lower(df_split.Last_Name))
)

df_email = df_with_email.withColumn(
    "email", 
    concat_ws("", df_with_email.email, lit("@syren.com"))
)
display(df_email)

# COMMAND ----------

# MAGIC %md write this dataframe to format as delta (use SaveAsTable)

# COMMAND ----------

df_email.write.format("delta").mode("overwrite").saveAsTable("email")

# COMMAND ----------

df_email.write.format("delta").mode("overwrite").saveAsTable("interns_adls.anusha.email")

# COMMAND ----------

# MAGIC %md
# MAGIC same output  write it to azure SQL

# COMMAND ----------


df_email.write.jdbc(url=jdbc_url, table="email", mode="overwrite", properties=properties)


# COMMAND ----------

# MAGIC %md
# MAGIC Explode,collect list,array,array_join
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, collect_list, array, array_join


# Sample data
data = [
    ("Antony", [1, 2, 3]),
    ("Antony", [1, 2, 3]),
    ("Bob", [4, 5]),
    ("kenn", [6, 7, 8]),
]

# Create DataFrame
df = spark.createDataFrame(data, ["name", "numbers"])

df.show(truncate=False)

# COMMAND ----------

df_exploded = df.select("name", explode("numbers").alias("number"))
df_exploded.show(truncate=False)

# COMMAND ----------

df_grouped = df.groupBy("name").agg(collect_list("numbers").alias("number_list"))
df_grouped.show(truncate=False)

# COMMAND ----------

df_exploded = df.select("name").collect()
display(df_exploded)

# COMMAND ----------

df_with_joined_array = df.withColumn("numbers_joined", array_join("numbers", ","))
df_with_joined_array.show(truncate=False)