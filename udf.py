# Databricks notebook source
simpleData = (("James", "Sales", 3000), \
    ("Michael", "Sales", 4600),  \
    ("Robert", "Sales", 4100),   \
    ("Maria", "Finance", 3000),  \
    ("James", "Sales", 3000),    \
    ("Scott", "Finance", 3300),  \
    ("Jen", "Finance", 3900),    \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000),\
    ("Saif", "Sales", 4100) \
  )
 
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def add_dollar_sign(salary):
    return '$' + str(salary)

# COMMAND ----------

add_dollar_sign_udf = udf(add_dollar_sign, StringType())

# COMMAND ----------

df.withColumn('salary', add_dollar_sign_udf(df['salary'])).display()