# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Define the schema for the table
schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("low_fats", StringType(), True),
    StructField("recyclable", StringType(), True)
])

# Data for the table
data = [
    (0, "Y", "N"),
    (1, "Y", "Y"),
    (2, "N", "Y"),
    (3, "Y", "Y"),
    (4, "N", "N")
]

# Create a DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## # 1757. Recyclable and Low Fat Products
# MAGIC
# MAGIC Write a solution to find the ids of products that are both low fat and recyclable.
# MAGIC
# MAGIC Return the result table in any order.

# COMMAND ----------

df.filter((df.low_fats == "Y") & (df.recyclable == "Y")).select("product_id").show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


# Define the schema for the Customer table
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("referee_id", IntegerType(), True)
])

# Data for the Customer table
data = [
    (1, "Will", None),
    (2, "Jane", None),
    (3, "Alex", 2),
    (4, "Bill", None),
    (5, "Zack", 1),
    (6, "Mark", 2)
]

# Create a DataFrame
df_customer = spark.createDataFrame(data, schema)

# Show the DataFrame
df_customer.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 584. Find Customer Referee
# MAGIC
# MAGIC Find the names of the customer that are not referred by the customer with id = 2.
# MAGIC Return the result table in any order.

# COMMAND ----------

df_customer.filter((df_customer.referee_id != 2) | (df_customer.referee_id.isNull())).select("id").show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

# Define the schema for the table
schema = StructType([
    StructField("name", StringType(), True),
    StructField("continent", StringType(), True),
    StructField("area", IntegerType(), True),
    StructField("population", IntegerType(), True),
    StructField("gdp", LongType(), True)
])

# Data for the table
data = [
    ("Afghanistan", "Asia", 652230, 25500100, 20343000000),
    ("Albania", "Europe", 28748, 2831741, 12960000000),
    ("Algeria", "Africa", 2381741, 37100000, 188681000000),
    ("Andorra", "Europe", 468, 78115, 3712000000),
    ("Angola", "Africa", 1246700, 20609294, 100990000000)
]

# Create a DataFrame
df.big_countries = spark.createDataFrame(data, schema)

# Show the DataFrame
df.big_countries.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 595. Big Countries
# MAGIC A country is big if:
# MAGIC
# MAGIC it has an area of at least three million (i.e., 3000000 km2), or
# MAGIC it has a population of at least twenty-five million (i.e., 25000000).
# MAGIC Write a solution to find the name, population, and area of the big countries.
# MAGIC
# MAGIC Return the result table in any order.

# COMMAND ----------

df.big_countries.filter((df.big_countries.area>=3000000) | (df.big_countries.population>=25000000)).show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DateType
from pyspark.sql import functions as F
from datetime import datetime


# Define the schema for the Views table
schema = StructType([
    StructField("article_id", IntegerType(), True),
    StructField("author_id", IntegerType(), True),
    StructField("viewer_id", IntegerType(), True),
    StructField("view_date", DateType(), True)
])

# Data for the Views table
data = [
    (1, 3, 5, "2019-08-01"),
    (1, 3, 6, "2019-08-02"),
    (2, 7, 7, "2019-08-01"),
    (2, 7, 6, "2019-08-02"),
    (4, 7, 1, "2019-07-22"),
    (3, 4, 4, "2019-07-21"),
    (3, 4, 4, "2019-07-21")
]

# Convert string to date format
data = [(article_id, author_id, viewer_id, datetime.strptime(view_date, "%Y-%m-%d").date()) for article_id, author_id, viewer_id, view_date in data]

# Create a DataFrame
df_views = spark.createDataFrame(data, schema)

# Show the DataFrame
df_views.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1148. Article Views I
# MAGIC Write a solution to find all the authors that viewed at least one of their own articles.
# MAGIC
# MAGIC Return the result table sorted by id in ascending order.
# MAGIC
# MAGIC The result format is in the following example.

# COMMAND ----------


df_views.filter(df_views.author_id==df_views.viewer_id).select(df_views.author_id).distinct().orderBy("author_id").show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark session
spark = SparkSession.builder.appName("CreateTableExample").getOrCreate()

# Define the schema for the table
schema = StructType([
    StructField("tweet_id", IntegerType(), True),
    StructField("content", StringType(), True)
])

# Create the DataFrame with the given data
data = [(1, "Let us Code"), 
        (2, "More than fifteen chars are here!")]

df = spark.createDataFrame(data, schema)

df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1683. Invalid Tweets
# MAGIC Write a solution to find the IDs of the invalid tweets. The tweet is invalid if the number of characters used in the content of the tweet is strictly greater than 15.
# MAGIC
# MAGIC Return the result table in any order.

# COMMAND ----------

from pyspark.sql.functions import length, col

content_length_df = df.select("tweet_id", length("content").alias("content_length"))
filtered_df = content_length_df.filter(col("content_length") > 15)
display(filtered_df)

# COMMAND ----------

from pyspark.sql.functions import length, when

# Select tweet_id and content length, and assign to a new DataFrame
content_length_df = df.select("tweet_id", length("content").alias("content_length"))

# Display the DataFrame
display(content_length_df)

# Filter the DataFrame where content_length is greater than 15
filtered_df = content_length_df.filter(content_length_df["content_length"] > 15)

# Display the filtered DataFrame
display(filtered_df)