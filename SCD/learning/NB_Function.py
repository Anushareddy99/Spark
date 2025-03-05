# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, array_join

spark = SparkSession.builder.appName("explode_example").getOrCreate()

data = [
    (1, ["apple", "banana", "cherry"]),
    (2, ["grape", "orange"]),
    (3, ["kiwi"])
]

df = spark.createDataFrame(data, ["id", "fruits"])

df.show(truncate=False)

exploded_df = df.withColumn("fruit", explode(df.fruits))

exploded_df.show(truncate=False)

# Stop the Spark session
spark.stop()

# COMMAND ----------

from pyspark.sql.functions import array_join

# Assuming df is already defined
df_with_joined_fruits = df.withColumn("joined_fruits", array_join(df.fruits, ", "))

# Show the result with the array joined into a string
display(df_with_joined_fruits)

# COMMAND ----------

from pyspark.sql import SparkSession

data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)


# COMMAND ----------

from pyspark.sql.functions import concat,col

df2=df.select(concat(df.firstname,df.middlename,df.lastname)
              .alias("FullName"),"dob","gender","salary")
df2.show(truncate=False)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import lpad, rpad

# Create a Spark session
spark = SparkSession.builder.appName("sparkbyexamples.com").getOrCreate()
data = [("John",), ("Jane",), ("Robert",)]
columns = ["name"]
df = spark.createDataFrame(data, columns)

# Left Padding (lpad)
df_with_left_padding = df.withColumn("left_padded_name", lpad(df["name"], 10, "vamshi"))

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

# MAGIC %md
# MAGIC ### ## continuation on pyspark sql functions like explode collect list ,array joins etc etc ...

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

arrayData = [
        ('James',['Java','Scala'],{'hair':'black','eye':'brown'}),
        ('Michael',['Spark','Java',None],{'hair':'brown','eye':None}),
        ('Robert',['CSharp',''],{'hair':'red','eye':'black'}),
        ('Washington',None,None),
        ('Jefferson',['1','2'],{})
    ]

df = spark.createDataFrame(data=arrayData, schema = ['name','knownLanguages','properties'])
df.printSchema()
df.show()

# COMMAND ----------

from pyspark.sql.functions import explode
df2 = df.select(df.name,explode(df.knownLanguages))#explode(df.properties))
#df2.printSchema()
df2.show()

# COMMAND ----------

from pyspark.sql.functions import explode
df3 = df.select(df.name,explode(df.properties))
df3.show()

# COMMAND ----------

from pyspark.sql.functions import explode_outer

df3 = df.select(df.name, explode_outer(df.properties))
df3.printSchema()
display(df3)

# COMMAND ----------

from pyspark.sql.functions import posexplode
df.select(df.name,posexplode(df.knownLanguages)).show()
df.select(df.name,posexplode(df.properties)).show()