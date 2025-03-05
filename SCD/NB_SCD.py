# Databricks notebook source
# MAGIC %md
# MAGIC ### SCD_Type-2

# COMMAND ----------

# list of employee data 
data = [["1001", "gaurav", "hyderabad","42000"], 
 ["1002", "vijay", "hyderabad","45565"], 
 ["1003", "akanksha","hyderabad", "52000"], 
 ["1004", "niharika", "hyderabad","35000"]] 
# specify column names 
columns = ['id','name','location','salry'] 
# creating a dataframe from the lists of data 
df_full = spark.createDataFrame(data, columns) 
# list of employee data 
data = [ ["1003", "akanksha","delhi", "65000"], 
 ["1004", "niharika", "bihar","10000"],
 ["1005", "murali","vijaywada", "80000"],
 ["1002", "vijay", "hyderabad","45565"]
 ] 
# specify column names 
columns = ['id','name','location','salry'] 
# creating a dataframe from the lists of data 
df_daily = spark.createDataFrame(data, columns)


# COMMAND ----------

display(df_full)
display(df_daily)

# COMMAND ----------

from pyspark.sql.functions import *
df_full=df_full.withColumn("Active_Flag",lit("Y")).withColumn("From_date",
to_date(current_date()))\
.withColumn("To_date",lit("Null"))
df_full.show()
df_daily=df_daily.withColumn("Active_Flage",lit("Y"))\
.withColumn("From_date",to_date(current_date()))\
.withColumn("To_date",lit("Null"))
df_daily.show()

# COMMAND ----------

update_ds = df_full.join(
    df_daily,
    (df_full.id == df_daily.id) & (df_full.Active_Flag == 'Y'),
    "inner"
).filter(
    hash(df_full.name, df_full.location, df_full.salry) != hash(df_daily.name, df_daily.location, df_daily.salry)
).select(
    df_full.id,
    df_full.name,
    df_full.location,
    df_full.salry,
    lit("N").alias("Active_Flag"),
    df_full.From_date,
    lit(to_date(current_date())).alias("To_Date")
)

display(update_ds)

# COMMAND ----------

no_change = df_full.join(
    update_ds,
    (df_full.id == update_ds.id) & (df_full.Active_Flag == 'Y'),
    'left_anti'
)
display(no_change)

# COMMAND ----------

insert_ds = df_daily.join(no_change,"id","left_anti")
insert_ds.show()

# COMMAND ----------

df_final=update_ds.union(insert_ds).union(no_change)
df_final.show()

# COMMAND ----------

# MAGIC %md SCD_Type-1

# COMMAND ----------


data = [["1001", "gaurav", "hyderabad","42000"], 
 ["1002", "vijay", "hyderabad","45565"], 
 ["1003", "akanksha","hyderabad", "52000"], 
 ["1004", "niharika", "hyderabad","35000"]] 

columns = ['id','name','location','salry'] 

df_full = spark.createDataFrame(data, columns) 

data = [ ["1003", "akanksha","delhi", "65000"], 
 ["1004", "niharika", "bihar","10000"],
 ["1005", "murali","vijaywada", "80000"],
 ["1002", "vijay", "hyderabad","45565"]
 ] 
 
columns = ['id','name','location','salry'] 

df_daily_update = spark.createDataFrame(data, columns)
print("Full data...")
df_full.show()
print("daily data...")
df_daily_update.show() 

# COMMAND ----------

from pyspark.sql.functions import coalesce

res = df_full.join(df_daily_update, "id", "full_outer").select(
    coalesce(df_full.id, df_daily_update.id).alias("ID"),
    coalesce(df_daily_update.name, df_full.name).alias("Name"),
    coalesce(df_daily_update.location, df_full.location).alias("Location"),
    coalesce(df_daily_update.salry, df_full.salry).alias("Salary")
)

display(res)