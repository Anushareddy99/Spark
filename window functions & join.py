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

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
windowSpec  = Window.partitionBy("department").orderBy("salary")

df.withColumn("row_number",row_number().over(windowSpec)) \
    .show(truncate=False)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank
windowSpec  = Window.partitionBy("department").orderBy("salary")
rank=df.withColumn("rank",rank().over(windowSpec))
display(rank)


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank,col
windowSpec  = Window.partitionBy("department").orderBy(col("salary").desc())
dense_rank_df=df.withColumn("dense_rank",dense_rank().over(windowSpec))
display(dense_rank_df)
percent_rank_df=df.withColumn("percent_rank",dense_rank().over(windowSpec)/df.count())
display(percent_rank_df)

# COMMAND ----------

from pyspark.sql.functions import ntile
df.withColumn("ntile",ntile(3).over(windowSpec)) \
    .show()

# COMMAND ----------

from pyspark.sql.functions import cume_dist    
df.withColumn("cume_dist",cume_dist().over(windowSpec)) \
   .show()


# COMMAND ----------

from pyspark.sql.functions import lag    
df.withColumn("lag",lag("salary",1).over(windowSpec)) \
      .show()

# COMMAND ----------

from pyspark.sql.functions import lead    
df.withColumn("lead",lead("salary",1).over(windowSpec)) \
    .show()

# COMMAND ----------

windowSpecAgg  = Window.partitionBy("department")
from pyspark.sql.functions import col,avg,sum,min,max,row_number 
df.withColumn("row",row_number().over(windowSpec)) \
  .withColumn("avg", avg(col("salary")).over(windowSpecAgg)) \
  .withColumn("sum", sum(col("salary")).over(windowSpecAgg)) \
  .withColumn("min", min(col("salary")).over(windowSpecAgg)) \
  .withColumn("max", max(col("salary")).over(windowSpecAgg)) \
  .where(col("row")==1).select("department","avg","sum","min","max") \
  .show()

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
empDF.show(truncate=False)


# COMMAND ----------

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

# COMMAND ----------

inner_join=empDF.join(deptDF, empDF.emp_dept_id==deptDF.dept_id, "inner")

display(inner_join)


# COMMAND ----------

left_join=empDF.join(deptDF, empDF.emp_dept_id==deptDF.dept_id, "left")
display(left_join)

# COMMAND ----------

right_join=empDF.join(deptDF, empDF.emp_dept_id==deptDF.dept_id, "right")
display(right_join)

# COMMAND ----------

anti_join=empDF.join(deptDF, empDF.emp_dept_id==deptDF.dept_id, "anti")
display(anti_join)

# COMMAND ----------

semi_join=empDF.join(deptDF, empDF.emp_dept_id==deptDF.dept_id, "semi")
display(semi_join)

# COMMAND ----------

full_join=empDF.join(deptDF, empDF.emp_dept_id==deptDF.dept_id, "full")
display(semi_join)

# COMMAND ----------

empDF.alias("emp1").join(empDF.alias("emp2"), \
    col("emp1.superior_emp_id") == col("emp2.emp_id"),"inner") \
    .select(col("emp1.emp_id"),col("emp1.name"), \
      col("emp2.emp_id").alias("superior_emp_id"), \
      col("emp2.name").alias("superior_emp_name")) \
   .show(truncate=False)