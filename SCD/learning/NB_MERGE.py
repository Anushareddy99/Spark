# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

data = [
    (1, 'John Doe', '123 Elm St.'),
    (2, 'Jane Doe', '456 Oak St.')
]

columns = ['CustomerID', 'Name', 'Address']

customers_df = spark.createDataFrame(data, columns)


customers_df.show()


new_data = [
    (1, 'John Doe', '789 Pine St.'),
    (2, 'Jane Doe', '456 Oak St.')
]

new_customers_df = spark.createDataFrame(new_data, columns)


new_customers_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Merge

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

data = [(1,"111"), (2,"222"), (3,"333"), (4,"444"), (5,"555")]

columns = ["id", "d_id"]

customers_df = spark.createDataFrame(data, columns)

new_data = ( [(1,"100"), (2,"200"), (6,"666"), (7,"777")])
   
columns = ["id", "d_id"]
new_customers_df = spark.createDataFrame(new_data, columns)

customers_df.show()
new_customers_df.show()

# COMMAND ----------

df_merge = customers_df.unionAll(new_customers_df)
df_merge.show() 

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
df_merge = df_merge.withColumn("_row_number", row_number().over(Window.partitionBy (df_merge['id']).orderBy('d_id')))
df_merge.show()

# COMMAND ----------

df_merge = df_merge.where(df_merge._row_number == 1).drop("_row_number")
df_merge.orderBy('id').show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

data = [
    (1, 'Michael', '123 Elm St.'),
    (2, 'Jane Doe', '456 Oak St.')
]

columns = ['CustomerID', 'Name', 'Address']

customers_df = spark.createDataFrame(data, columns)


customers_df.show()


new_data = [
    (1, 'John Doe', '789 Pine St.'),
    (2, 'Jane Doe', '456 Oak St.'),
    (3, 'Jhon Hel', '789 Pie Str.'),
]

new_customers_df = spark.createDataFrame(new_data, columns)


new_customers_df.show()

# COMMAND ----------

customers_df.write.format("delta").option("header",True).saveAsTable("interns_adls.Anusha.Merge")

# COMMAND ----------

# MAGIC %sql
# MAGIC select* from customers

# COMMAND ----------

new_customers_df.write.format("delta").option("header",True).saveAsTable("interns_adls.Anusha.Merge_1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from NewCustomers

# COMMAND ----------


spark.sql("""MERGE INTO customers AS target
USING NewCustomers AS source
ON target.CustomerID = source.CustomerID
WHEN MATCHED THEN
  UPDATE SET
    target.CustomerID = source.CustomerID,
    target.Name = source.Name,
    target.Address = source.Address
WHEN NOT MATCHED THEN
INSERT (CustomerID, Name, Address)
VALUES (source.CustomerID, source.Name, source.Address)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into customers
# MAGIC using NewCustomers
# MAGIC on customers.CustomerID = NewCustomers.CustomerID
# MAGIC when matched then
# MAGIC update set customers.CustomerID = NewCustomers.CustomerID,
# MAGIC     customers.Name = NewCustomers.Name,
# MAGIC     customers.Address = NewCustomers.Address
# MAGIC when not matched then                                                           --we use insert when there are new rows in source but not in target
# MAGIC insert (CustomerID, Name, Address)
# MAGIC values (NewCustomers.CustomerID, NewCustomers.Name, NewCustomers.Address)
# MAGIC when not matched by source then                                                 --deletes the rows from target which are not in source
# MAGIC delete;