# Databricks notebook source
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

# MAGIC %md
# MAGIC

# COMMAND ----------

display(schema)

# COMMAND ----------

df.printSchema()

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

# Function to rename columns
def rename_columns(df):
    new_columns = []
    
    for col in df.columns:
        new_col = col.replace(" ", "_")
        
        if new_col.lower() == 'climate_category':
            new_col = 'cc'
        
        if new_col.lower() == 'property_price_to_income_value':
            new_col = 'PropertyPricetoIncomeValue'
        
        new_columns.append(new_col)
    
    for old_col, new_col in zip(df.columns, new_columns):
        df = df.withColumnRenamed(old_col, new_col)
    
    return df

df_renamed = rename_columns(df)

display(df_renamed)