# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat,col
data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)

# Using select() with concat()
df2=df.select(concat(df.firstname,df.middlename,df.lastname)
              .alias("FullName"),"dob","gender","salary")
df2.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import concat_ws,col
df3=df.select(concat_ws('_',df.firstname,df.middlename,df.lastname)
              .alias("FullName"),"dob","gender","salary")
df3.display()

# COMMAND ----------

from pyspark.sql.functions import expr

df_with_substr = df3.withColumn("substr_example", expr("substr(FullName, 2, length(FullName))"))
df_with_substr1 = df3.withColumn("substr_example", expr("substr(FullName, 2, 4)"))

# Show the DataFrame
display(df_with_substr)
display(df_with_substr1)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import lpad, rpad

# Create a Spark session
spark = SparkSession.builder.appName("sparkbyexamples.com").getOrCreate()
data = [("John",), ("Jane",), ("Robert",)]
columns = ["name"]
df = spark.createDataFrame(data, columns)

# Left Padding (lpad)
df_with_left_padding = df.withColumn("left_padded_name", lpad(df["name"], 10, "x"))

# Right Padding (rpad)
df_with_right_padding = df.withColumn("right_padded_name", rpad(df["name"], 10, "X"))

df_with_left_padding.show()
df_with_right_padding.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import split

# Sample DataFrame
data = [("John,Doe",), ("Jane,Smith",), ("Robert,Johnson",)]
columns = ["full_name"]

df = spark.createDataFrame(data, columns)

# Use the split function to split the "full_name" column by comma
split_columns = split(df["full_name"], ",")

# Add the split columns to the DataFrame
df_with_split = df.withColumn("first_name", split_columns[0]).withColumn("last_name", split_columns[1])

# Show the DataFrame
df_with_split.show()

# COMMAND ----------

# Specify the string to check for
substring_to_check = "Smith"

# Use filter and contains to check if the column contains the specified substring
filtered_df = df.filter(col("full_name").contains(substring_to_check))

# Show the DataFrame
filtered_df.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace

# Sample DataFrame
data = [("John Doe",), ("Jane Smith",), ("Robert Johnson",)]
columns = ["full_name"]

df = spark.createDataFrame(data, columns)

# Use regexp_replace to replace spaces with underscores
df_replaced = df.withColumn("name_with_underscore", regexp_replace(df["full_name"], r' ', '_'))

# Show the DataFrame
df_replaced.show()


# COMMAND ----------

emptyRDD = spark.createDataFrame([], schema='string')
print(emptyRDD)