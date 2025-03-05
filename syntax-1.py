# Databricks notebook source
simpleData = (("Java",4000,5), \
("Python", 4600,10), \
("Scala", 4100,15), \
("Scala", 4500,15), \
("PHP", 3000,20), \
)
columns= ["CourseName", "fee", "discount"]
# Create DataFrame
df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import upper
def to_upper_str_columns(df):
  return df.withColumn("CourseName",upper(df.CourseName))
df_uppercase = to_upper_str_columns(df)
df_uppercase.display()

# COMMAND ----------

_from pyspark.sql.functions import lower
def to_lower_str_columns(df):
  return df.withColumn("CourseName",lower(df.CourseName))
df_lowercase = to_lower_str_columns(df)
df_lowercase.display()

# COMMAND ----------

reduceby=100

def reduce_price(df,reduceBy):
    return df.withColumn("new_fee",df.fee - reduceBy)
df_reduced = reduce_price(df,reduceBy)

df_reduced.display()   

# COMMAND ----------

#from pyspark.sql import functions as F

# Define the reduceBy value
reduceBy = 10  # Example value, you can change it to whatever amount you want to reduce

def reduce_price(df, reduceBy):
    return df.withColumn("new_fee", df.fee - reduceBy)

# Assuming df is already a DataFrame
df_reduced = reduce_price(df, reduceBy)
df_reduced.show()

# COMMAND ----------

def apply_discount(df):    
    return df.withColumn("discounted_fee", df.new_fee - (df.new_fee * df.discount) / 100)
df_discounted = apply_discount(df_reduced)
display(df_discounted)

# COMMAND ----------

df2 = df.transform(to_upper_str_columns) \
.transform(reduce_price,1000) \
.transform(apply_discount)
df2.show()

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("Example").getOrCreate()

# Your data list
data = ["Project","Gutenberg’s","Alice’s","Adventures",
        "in","Wonderland","Project","Gutenberg’s","Adventures",
        "in","Wonderland","Project","Gutenberg’s"]

# Create a DataFrame from the list of data
df = spark.createDataFrame([(x,) for x in data], ["word"])

# Show the DataFrame
df.show()


# COMMAND ----------

from pyspark.sql.functions import explode, split

df_split = df.withColumn("word", explode(split(df["word"], " ")))
for element in df_split.collect():
    print(element['word'])


# COMMAND ----------

data = [('James','Smith','M',30),
('Anna','Rose','F',41),
('Robert','Williams','M',62),
]
columns = ["firstname","lastname","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
df.show()

# COMMAND ----------

from pyspark.sql.functions import concat, lit, col

df_transformed = df.withColumn("name", concat(col("firstname"), lit(","), col("lastname"))) \
                   .withColumn("new_salary", col("salary") * 2) \
                   .select("name", "gender", "new_salary")

# Show the result
display(df_transformed)

# COMMAND ----------

columns = ["Seqno", "Name"]
data = [
    ("1", "john jones"),
    ("2", "tracey smith"),
    ("3", "amy sanders")
]
# Create DataFrame
df = spark.createDataFrame(data=data, schema=columns)
df.show()

# foreach() Example
def f(df):
    print(df.Seqno)

df.foreach(f)

# COMMAND ----------

df=spark.range(100)
print(df.sample(0.3).collect())

# COMMAND ----------

# MAGIC %md
# MAGIC pivot

# COMMAND ----------

data1 = [('ABC','Q1',6000),
('XYZ','Q1',5000),
('ABC','Q2',8000),
('XYZ','Q2',7000)]
schema1 = ['Company','Quarter','Revenue']

df1 = spark.createDataFrame(data = data1,schema = schema1)


# COMMAND ----------

df1.display()

# COMMAND ----------

from pyspark.sql.functions import lit

# Assuming you want to add an empty 'Email' column initially
df1_with_email = df1.withColumn("Email", lit(None).cast("string"))

# Save the modified DataFrame as the new table
df1_with_email.write.mode("overwrite").saveAsTable("df1_table")

# COMMAND ----------

df1_with_email.show()

# COMMAND ----------

df2=df1.drop("Revenue")
df2databricksforinterns.default.df1_table.show()

# COMMAND ----------

spark.sql("ALTER TABLE databricksforinterns.default.df1_table DROP COLUMN Revenue")

# COMMAND ----------

df1.describe().show()

# COMMAND ----------

df1.describe(['revenue']).show()

# COMMAND ----------

df2 = df1.groupBy('Company').pivot('Quarter').sum('Revenue')
df2.show()

# COMMAND ----------

df3 = df2.selectExpr(
    'company','stack(2,"Q1",Q1,"Q2",Q2) as (Quarter,Revenue)').where("Revenue is not null")
display(df3)

# COMMAND ----------

