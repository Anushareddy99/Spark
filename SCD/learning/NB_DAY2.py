# Databricks notebook source
# MAGIC %fs ls "abfss://source@internsstorage1.dfs.core.windows.net/anusha/"

# COMMAND ----------

# MAGIC %md
# MAGIC Reading CSV

# COMMAND ----------

df=(spark.read
            .format("csv")
            .option("header", "true")
            .option("quote","'")
            .schema(schema)
            .load('abfss://source@internsstorage1.dfs.core.windows.net/anusha/Quality_of_Life.csv') )
display(df)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, FloatType

schema = StructType([
    StructField("country", StringType(), True),
    StructField("Purchasing Power Value", FloatType(), True),
    StructField("Purchasing Power Category", StringType(), True),
    StructField("Safety Value", FloatType(), True),
    StructField("Safety Category", StringType(), True),
    StructField("Health Care Value", FloatType(), True),
    StructField("Health Care Category", StringType(), True),
    StructField("Climate Value", FloatType(), True),
    StructField("Climate Category", StringType(), True),
    StructField("Cost of Living Value", FloatType(), True),
    StructField("Cost of Living Category", StringType(), True),
    StructField("Property Price to Income Value", FloatType(), True),
    StructField("Property Price to Income Category", StringType(), True),
    StructField("Traffic Commute Time Value", FloatType(), True),
    StructField("Traffic Commute Time Category", StringType(), True),
    StructField("Pollution Value", FloatType(), True),
    StructField("Pollution Category", StringType(), True),
    StructField("Quality of Life Value", FloatType(), True),
    StructField("Quality of Life Category", StringType(), True)
])

# COMMAND ----------

display(schema)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Transformation

# COMMAND ----------

cleaned_columns = {}
for column in df.columns:
    cleaned_columns[column] = column.replace(" ", "_")

New_named_df = df
for i in range(len(df.columns)):
    New_named_df = New_named_df.withColumnRenamed(
        df.columns[i], 
        cleaned_columns[df.columns[i]]
    )

display(New_named_df)

# COMMAND ----------

high_health_care=df.filter(df['Health Care Category'] == 'Very High')
display(high_health_care)


# COMMAND ----------

df.groupby(df['Safety Category']).agg(
    count(df["Safety Category"]).alias("count Safety Category")
).display()

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import when, col

New_named_df= New_named_df.withColumn(
    "transform_Safety_Category",
    when(col("Safety_Category") == "very High", 6)
    .when(col("Safety_Category") == "High", 5)
    .when(col("Safety_Category") == "Moderate", 4)
    .when(col("Safety_Category") == "Low", 3)
    .when(col("Safety_Category") == "Very Low", 2)
    .otherwise(1)
)

display(New_named_df)

# COMMAND ----------

from pyspark.sql.functions import lit
new_column_df=New_named_df.withColumn("percentage", lit(100))
display(new_column_df)

# COMMAND ----------

#rename_df=new_column_df.withColumnRenamed("Safety Category","Safety_Category")\
    #.withColumnRenamed("Health Care Category","Health_Care_Category")\
    #.withColumnRenamed("Climate Category","Climate_Category")\
    #.withColumnRenamed("Cost of Living Category","Cost_of_Living_Category")\
    #.withColumnRenamed("Property Price to Income Category","Property_Price_to_Income_Category")\
    #.withColumnRenamed("Traffic Commute Time Category","Traffic_Commute_Time_Category")\
    #.withColumnRenamed("Pollution Category","Pollution_Category")\
    #.withColumnRenamed("Purchasing Power Category","Purchasing_Power_Category")\
    #.withColumnRenamed("country","Country")\
    #.withColumnRenamed("Purchasing Power Value","Purchasing_Power_Value")\
    #.withColumnRenamed("Safety Value","Safety_Value")\
    #.withColumnRenamed("Health Care Value","Health_Care_Value")\
    #.withColumnRenamed("Climate Value","Climate_Value")\
    #.withColumnRenamed("Cost of Living Value","Cost_of_Living_Value")\
    #.withColumnRenamed("Property Price to Income Value","Property_Price_to_Income_Value")\
    #.withColumnRenamed("Traffic Commute Time Value","Traffic_Commute_Time_Value")\
    #.withColumnRenamed("Pollution Value","Pollution_Value")\
    #.withColumnRenamed("percentage","Percentage")\
    #.withColumnRenamed("Quality of Life Value","Quality_of_Life_Value")\
    #.withColumnRenamed("Quality of Life Category","Quality_of_Life_Category")
#display(rename_df)

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

New_named_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("abfss://source@internsstorage1.dfs.core.windows.net/anusha/Quality_of_Life_newdelta.delta")

# COMMAND ----------

spark.sql("DESCRIBE HISTORY delta.`abfss://source@internsstorage1.dfs.core.windows.net/anusha/Quality_of_Life_newdelta.delta`")

# COMMAND ----------

from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark,"abfss://source@internsstorage1.dfs.core.windows.net/anusha/Quality_of_Life_newdelta.delta")

history = delta_table.history()
display(history)

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

distinct_df = new_column_df.select("Climate_Category").distinct()
display(distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC