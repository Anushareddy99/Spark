# Databricks notebook source
# MAGIC %fs ls "abfss://source@internsstorage1.dfs.core.windows.net/anusha/"

# COMMAND ----------

# MAGIC %md
# MAGIC Reading CSV

# COMMAND ----------

df=(spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("quote","'")
            .load('abfss://source@internsstorage1.dfs.core.windows.net/anusha/Quality_of_Life.csv') )
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Transformation

# COMMAND ----------

high_health_care=df.filter(df['Health Care Category'] == 'Very High')
display(high_health_care)


# COMMAND ----------

from pyspark.sql.functions import max, min, count

df.select(max(df["Health Care Value"]).alias("max Health Care Value")).display()
df.select(min(df["Health Care Value"]).alias("min Health Care Value")).display()

# COMMAND ----------

df.groupby(df['Safety Category']).agg(
    count(df["Safety Category"]).alias("count Safety Category")
).display()

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import when, col

df = df.withColumn(
    "transform_Safety_Category",
    when(col("Safety Category") == "very High", 6)
    .when(col("Safety Category") == "High", 5)
    .when(col("Safety Category") == "Moderate", 4)
    .when(col("Safety Category") == "Low", 3)
    .when(col("Safety Category") == "Very Low", 2)
    .otherwise(1)
)

display(df)

# COMMAND ----------

from pyspark.sql.functions import lit
new_column_df=df.withColumn("percentage", lit(100))
display(new_column_df)

# COMMAND ----------

rename_df=new_column_df.withColumnRenamed("Safety Category","Safety_Category")\
    .withColumnRenamed("Health Care Category","Health_Care_Category")\
    .withColumnRenamed("Climate Category","Climate_Category")\
    .withColumnRenamed("Cost of Living Category","Cost_of_Living_Category")\
    .withColumnRenamed("Property Price to Income Category","Property_Price_to_Income_Category")\
    .withColumnRenamed("Traffic Commute Time Category","Traffic_Commute_Time_Category")\
    .withColumnRenamed("Pollution Category","Pollution_Category")\
    .withColumnRenamed("Purchasing Power Category","Purchasing_Power_Category")\
    .withColumnRenamed("country","Country")\
    .withColumnRenamed("Purchasing Power Value","Purchasing_Power_Value")\
    .withColumnRenamed("Safety Value","Safety_Value")\
    .withColumnRenamed("Health Care Value","Health_Care_Value")\
    .withColumnRenamed("Climate Value","Climate_Value")\
    .withColumnRenamed("Cost of Living Value","Cost_of_Living_Value")\
    .withColumnRenamed("Property Price to Income Value","Property_Price_to_Income_Value")\
    .withColumnRenamed("Traffic Commute Time Value","Traffic_Commute_Time_Value")\
    .withColumnRenamed("Pollution Value","Pollution_Value")\
    .withColumnRenamed("percentage","Percentage")\
    .withColumnRenamed("Quality of Life Value","Quality_of_Life_Value")\
    .withColumnRenamed("Quality of Life Category","Quality_of_Life_Category")
display(rename_df)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC parquet and Delta

# COMMAND ----------

new_column_df.write.parquet(
    "abfss://source@internsstorage1.dfs.core.windows.net/anusha/Quality_of_Life.parquet", 
    mode="overwrite"
)

# COMMAND ----------

rename_df.write.format("delta").mode("overwrite").save("abfss://source@internsstorage1.dfs.core.windows.net/anusha/Quality_of_Life.delta")

# COMMAND ----------

# MAGIC %md
# MAGIC Partition and bucketing

# COMMAND ----------

df.write.option("header", True) \
        .partitionBy("Safety Category") \
        .mode("overwrite") \
        .parquet("abfss://source@internsstorage1.dfs.core.windows.net/anusha/Quality_of_Life.parquet")

# COMMAND ----------

df.write.option("header", True) \
        .partitionBy("Health Care Category") \
        .mode("overwrite") \
        .parquet("abfss://source@internsstorage1.dfs.core.windows.net/anusha/Quality_of_Life_partition.parquet")

# COMMAND ----------

rename_df.write\
    .format("delta")\
    .partitionBy("Cost_of_Living_Category")\
    .mode("overwrite")\
    .save("abfss://source@internsstorage1.dfs.core.windows.net/anusha/Quality_of_Life_delta.delta")

# COMMAND ----------

df.write \
  .format("parquet") \
  .bucketBy(6, "Climate Category") \
  .sortBy("Climate Category") \
  .option("path", "abfss://source@internsstorage1.dfs.core.windows.net/anusha/Quality_of_Life_bucketing.parquet") \
  .mode("overwrite")\
  .saveAsTable("bucketing_table")

# COMMAND ----------

# MAGIC %md
# MAGIC