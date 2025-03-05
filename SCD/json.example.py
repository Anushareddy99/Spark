# Databricks notebook source
iris_schema="sepalLength String,sepalWidth String,petalLength String,petalWidth String,species String"

# COMMAND ----------

iris_df=spark.read\
    .option("multiLine",True)\
    .schema(iris_schema)\
    .json("abfss://source@internsstorage1.dfs.core.windows.net/anusha/iris.json")

# COMMAND ----------

display(iris_df)

# COMMAND ----------

iris_df.printSchema()

# COMMAND ----------

display(iris_df)

# COMMAND ----------

# MAGIC %md
# MAGIC drop unwanted coloumns from the dataframe

# COMMAND ----------

iris_drop=iris_df.drop('sepalLength')

# COMMAND ----------

# MAGIC %md
# MAGIC Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

iris_df=iris_drop.withcolumnrenamed("sepalLength","sepal_Length")\
   .withcolumnrenamed("sepalWidth","sepal_width")\
   .withcolumn("timestamp",current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC write output to parquet file

# COMMAND ----------

"name":{"forename":"John","surname":"Doe"}
read the json the using the sparkdataframe
name_schema=structType(fields=[structField("forename",StringType(),True),structField("surname",StringType(),True)])