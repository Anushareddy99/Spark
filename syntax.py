# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType
from decimal import Decimal

# Define the schema for the DataFrame
schema = StructType([
    StructField("roll_no", IntegerType(), nullable=False),
    StructField("student_name", StringType(), nullable=True),
    StructField("gender", StringType(), nullable=True),
    StructField("fee", DecimalType(10, 2), nullable=True),
    StructField("marks", IntegerType(), nullable=True),
    StructField("percentage", IntegerType(), nullable=True)
])

# Define the data to be inserted as a list of tuples
data = [
    (1, 'John Doe', 'Male', Decimal('8500.00'), 425, 85),
    (2, 'Jane Smith', 'Female', Decimal('9500.00'), 410, 82),
    (3, 'Robert Brown', 'Male', Decimal('11000.00'), 300, 60),
    (4, 'Alice Johnson', 'Female', Decimal('10500.00'), 450, 90),
    (5, 'Michael Davis', 'Male', Decimal('7800.00'), 380, 76),
    (6, 'Emily Wilson', 'Female', Decimal('9200.00'), 395, 79),
    (7, 'David Martinez', 'Male', Decimal('12000.00'), 470, 94),
    (8, 'Sophia Lee', 'Female', Decimal('10000.00'), 440, 88),
    (9, 'Daniel Anderson', 'Male', Decimal('10500.00'), 405, 81),
    (10, 'Olivia Harris', 'Female', Decimal('9800.00'), 460, 92),
    (11, 'Lucas Young', 'Male', Decimal('9300.00'), 315, 63),
    (12, 'Mia King', 'Female', Decimal('10600.00'), 400, 80),
    (13, 'Benjamin Scott', 'Male', Decimal('11500.00'), 450, 90),
    (14, 'Isabella Green', 'Female', Decimal('8800.00'), 375, 75),
    (15, 'Ethan Adams', 'Male', Decimal('9800.00'), 420, 84),
    (16, 'Ava Baker', 'Female', Decimal('8700.00'), 350, 70),
    (17, 'James Nelson', 'Male', Decimal('9500.00'), 480, 96),
    (18, 'Charlotte Carter', 'Female', Decimal('10200.00'), 425, 85),
    (19, 'Henry Perez', 'Male', Decimal('9400.00'), 375, 75),
    (20, 'Amelia Roberts', 'Female', Decimal('8900.00'), 395, 79),
    (21, 'Matthew Evans', 'Male', Decimal('10500.00'), 460, 92),
    (22, 'Sophia Hall', 'Female', Decimal('9800.00'), 410, 82),
    (23, 'Jack Turner', 'Male', Decimal('8900.00'), 360, 72),
    (24, 'Lily Morgan', 'Female', Decimal('11200.00'), 485, 97),
    (25, 'Daniel Martinez', 'Male', Decimal('10100.00'), 440, 88),
    (26, 'Chloe Lee', 'Female', Decimal('9200.00'), 395, 79),
    (27, 'Alexander Allen', 'Male', Decimal('10500.00'), 410, 82),
    (28, 'Madeline Perez', 'Female', Decimal('9500.00'), 350, 70)
]

# Create the DataFrame with the defined schema
df = spark.createDataFrame(data, schema)

# Show the DataFrame schema
display(df)

# COMMAND ----------

#df.write.saveAsTable("interns_adls.anusha.df")

# COMMAND ----------

df.display("roll_no")

# COMMAND ----------

df.describe()

# COMMAND ----------

df.filter(df.roll_no == 1).display()

# COMMAND ----------

from pyspark.sql import functions as F; df.select(df.student_name, F.when(df.gender == 'Male', 1).otherwise(0).alias('is_male')).show()

# COMMAND ----------

df.select("student_name","gender","fee").display()

# COMMAND ----------

res = df[df.student_name.isin(["Madeline Perez"])]
display(res)

# COMMAND ----------

df.select(df.student_name.like('Lucas Young')).show()

# COMMAND ----------

 j_start = df.select(df.student_name.startswith('James'))
 j_start.display()

# COMMAND ----------

 df.select(df.student_name.endswith("Lee")).show()

# COMMAND ----------

 df.select(df.student_name.substr(1, 3).alias("substr")).collect()

# COMMAND ----------

df.select(df.fee, df.fee.between(8800,10000)).display()


# COMMAND ----------

 fee_rename_df = df.withColumnRenamed('fee','fees')

# COMMAND ----------

display(fee_rename_df)

# COMMAND ----------

df.replace({350: 400}, subset=["marks"]).display()

# COMMAND ----------

df.filter(df["gender"].isNotNull())

# COMMAND ----------

from pyspark.sql.functions import col,lit   
df_fees = df.withColumn("fees",col("fees").cast("Integer"))
df_fees.display()


# COMMAND ----------

df.withColumn("fees",col("fees")*100).show()

# COMMAND ----------

df.withColumn("CopiedColumn",col("fees")* -1).show()

# COMMAND ----------

lit_fees_df=df.withColumn("contest",lit("12000"))
display(lit_fees_df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

drop_lit=lit_fees_df.drop("contest")


# COMMAND ----------

drop_lit.display()

# COMMAND ----------

df.filter( (df.percentage >= 70) & (df.gender == "Male") ).show(truncate=False)

# COMMAND ----------

l = [92,72,88,90,85]
df.filter(df.percentage.isin(l)).show()


# COMMAND ----------

data = [("James", "Sales", 3000),
("Michael", "Sales", 4600),
("Robert", "Sales", 4100),
("Maria", "Finance", 3000),
("James", "Sales", 3000),
("Scott", "Finance", 3300),
("Jen", "Finance", 3900),
("Jeff", "Marketing", 3000),
("Kumar", "Marketing", 2000),
("Saif", "Sales", 4100)
]
# Create DataFrame
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = data, schema = columns)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

print(df.count())
df.show()

# COMMAND ----------

distinctDF = df.distinct()
print(distinctDF.count())
distinctDF.show()


# COMMAND ----------

df2 = df.dropDuplicates()
print(df2.count())
df2.show()

# COMMAND ----------

simpleData = [("James","Sales","NY",90000,34,10000),
("Michael","Sales","NY",86000,56,20000),
("Robert","Sales","CA",81000,30,23000),
("Maria","Finance","CA",90000,24,23000),
("Raman","Finance","CA",99000,40,24000),
("Scott","Finance","NY",83000,36,19000),
("Jen","Finance","NY",79000,53,15000),
("Jeff","Marketing","CA",80000,25,18000),
("Kumar","Marketing","NY",91000,50,21000)
]
columns= ["employee_name","department","state","salary","age","bonus"]
37
df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

df.sort("employee_name", "department", ascending=[True, False]).show()

# COMMAND ----------

df.sort('department','state').show()


# COMMAND ----------

df.orderBy('department').show()

# COMMAND ----------

from pyspark.sql.functions import col
df.orderBy(col("department"),col("state")).show(truncate=False)

# COMMAND ----------

df.sort(df.department.asc(),df.state.asc()).show(truncate=False)


# COMMAND ----------

df.sort(df.department.desc(),df.state.desc()).show(truncate=False)

# COMMAND ----------

df.sort(col("department").desc(),col("state").desc()).show(truncate=False)

# COMMAND ----------

simpleData = [("James","Sales","NY",90000,34,10000),
("Michael","Sales","NY",86000,56,20000),
("Robert","Sales","CA",81000,30,23000),
("Maria","Finance","CA",90000,24,23000)
]
columns= ["employee_name","department","state","salary","age","bonus"]
df1 = spark.createDataFrame(data = simpleData, schema = columns)
print("dataframe 1:")
df1.printSchema()
df1.show(truncate=False)
simpleData2 = [("James","Sales","NY",90000,34,10000),
("Maria","Finance","CA",90000,24,23000),
("Jen","Finance","NY",79000,53,15000),
("Jeff","Marketing","CA",80000,25,18000),
("Kumar","Marketing","NY",91000,50,21000)
]
columns2= ["employee_name","department","state","salary","age","bonus"]
df2 = spark.createDataFrame(data = simpleData2, schema = columns2)
print("dataframe 2:")
df2.printSchema()
df2.show(truncate=False)

# COMMAND ----------

unionDF = df1.union(df2)
print('unionDF')
unionDF.show()

# COMMAND ----------

disDF = df.union(df2).distinct()
disDF.show(truncate=False)

# COMMAND ----------

columns = ["Seqno","Name"]
data = [("1", "john jones"),
("2", "tracey smith"),
("3", "amy sanders")]
df = spark.createDataFrame(data=data,schema=columns)
df.show(truncate=False)

# COMMAND ----------

def convertCase(str):
resStr=""
arr = str.split(" ")
for x in arr:
resStr= resStr + x[0:1].upper() + x[1:len(x)] + " "
return resStr

# COMMAND ----------

from pyspark.sql.functions import col,udf
from pyspark.sql.types import StringType
convertUDF = udf(lambda z : convertCase(z),StringType())

# COMMAND ----------

df.withColumn('Convert Case Name',convertUDF(col('name'))).show(truncate=False)

# COMMAND ----------

@udf(returnType=StringType())
def upperCase(str):
    return str.upper()
df.withColumn('Cureated Name',upperCase(col('Name'))).show(truncate = False)