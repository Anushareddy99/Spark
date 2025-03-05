# Databricks notebook source
# MAGIC %md
# MAGIC -  
# MAGIC a) continue joins practice in pyspark and sql (left,left-anti,full-outer)
# MAGIC -  b) read excel file in databricks 
# MAGIC -   c) use case : create a function which gives count of records from each table present in SQL Server

# COMMAND ----------

# MAGIC %md
# MAGIC a) continue joins practice in pyspark and sql (left,left-anti,full-outer)

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession

emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)
empDF.printSchema()
empDF.show()

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show()

# COMMAND ----------

left_join_df = empDF.join(
    deptDF, 
    empDF.emp_dept_id == deptDF.dept_id, 
    'left'
)
display(left_join_df)

# COMMAND ----------

full_outer_join_df = empDF.join(
    deptDF, 
    empDF.emp_dept_id == deptDF.dept_id, 
    'full_outer'
)
display(left_join_df)

# COMMAND ----------

# MAGIC %md
# MAGIC b) read excel file in databricks

# COMMAND ----------

# MAGIC %fs ls 'abfss://source@internsstorage1.dfs.core.windows.net/anusha/'

# COMMAND ----------

pip install openpyxl

# COMMAND ----------

pip install fsspec

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------


import pyspark.pandas as pd

pdf=pd.read_excel("abfss://source@internsstorage1.dfs.core.windows.net/anusha/customer.xlsx")
print(pdf)

# COMMAND ----------

spark_df=pdf.to_spark()

# COMMAND ----------

spark_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC c)use case : create a function which gives count of records from each table present in SQL Server

# COMMAND ----------

jdbc_url = "jdbc:sqlserver://azure-intern-sqlserver.database.windows.net:1433;database=azure-sql-intern-db-01"

tables_query = """
SELECT TABLE_SCHEMA, TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
"""
tables_df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("query", tables_query)
    .option("user", "intern")
    .option("password", "Syren@123")
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .load()
)

row_count_list = []
for row in tables_df.collect():
    tb_schema = row['TABLE_SCHEMA']
    tb_name = row['TABLE_NAME']
    row_count_query = f"""
                      SELECT COUNT(*) AS Row_Count FROM [{tb_schema}].[{tb_name}]
                      """
    count_df = spark.read.jdbc(
        jdbc_url,
        f"({row_count_query}) as count_table",
        properties={
            "user": "intern",
            "password": "Syren@123",
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }
    )
    row_count = count_df.collect()[0]['Row_Count']
    row_count_list.append((tb_schema, tb_name, row_count))

final_df = spark.createDataFrame(row_count_list, ["Schema", "Table", "RowCount"])
display(final_df)