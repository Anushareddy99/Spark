# Databricks notebook source
# MAGIC %md
# MAGIC d)joins practice in pyspark  (left , left -anti,left-semi  etc.)

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

empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner").show()

# COMMAND ----------

empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"left").show()

# COMMAND ----------

empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"semi").show()

# COMMAND ----------

empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"anti").show()

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
display(full_outer_join_df)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC dbutils

# COMMAND ----------

dbutils.widgets.dropdown(name="fruitsdrop", defaultValue="apple", choices=["apple", "banana", "orange"])

# COMMAND ----------

dbutils.widgets.combobox(name="vegetablecombo", defaultValue="carrot", choices=["carrot", "broccoli", "potato"])

# COMMAND ----------

dbutils.widgets.multiselect(
    name="meatmultiselect",
    defaultValue="chicken",
    choices=["chicken", "beef", "pork"]
)

# COMMAND ----------

dbutils.widgets.text(name="fruittext", defaultValue="apple")

# COMMAND ----------

dbutils.widgets.text("fruitdrop", "default_value", "Fruit Drop")
fruitdrop_value = dbutils.widgets.get("fruitdrop")
display(fruitdrop_value)

# COMMAND ----------

dbutils.widgets.get("fruitdrop")

# COMMAND ----------

dbutils.widgets.help()