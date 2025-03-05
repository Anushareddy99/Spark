# Databricks notebook source
#1. import the give dataframe & format the price column from the below table to display price as a string with 2 decimal places 
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_number

data = [("MAT0101KGF2 ", 4973.6 )]
columns = ["Material_Name ", "price"]
df=spark.createDataFrame(data, columns)

df.show()


# COMMAND ----------

df = df.withColumn("format_price", format_number("price", 2))
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC  #2.import the given dataframe and remove leading and trailing spaces from the below given table’s comments column.  

# COMMAND ----------

data = [("MAT0101KGF2 ", "Exce  llen   t     Material       * " )]
columns = ["Material_Name ", "Comments "]
df=spark.createDataFrame(data, columns)
df.show()

# COMMAND ----------

from pyspark.sql.functions import concat, trim

remove_spaces = df.withColumn('spacesComments',trim(df['Comments ']))

display(remove_spaces)

# COMMAND ----------

# MAGIC %md
# MAGIC 3) import the below dataframe to replace nulls in the column name with "n/a" and rename the column with name_updated  

# COMMAND ----------

data = [(1, "GHJ ", "HR "),
        (2, "IUY", "IT"),
        (3, "GHJLK", None),
        (4, "ERT", "Finance"),
        (5, None, "IT")]
columns = ["emp_id", "Name", "department"]
df = spark.createDataFrame(data, columns)
display(df)

# COMMAND ----------

from pyspark.sql.functions import when, col

remove_null_name = df.withColumn("Name", when(col("Name").isNull(), "N/a").otherwise(col("Name")))
remove_Null_department=remove_null_name.withColumn("department", when(col("department").isNull(), "N/a").otherwise(col("department")))
rename_name=df.withColumnRenamed("Name", "GJL")
rename_department=df.withColumnRenamed("department", "Finance")
display(remove_null_name)



# COMMAND ----------

# MAGIC %md
# MAGIC import the given dataframe to filter rows where the email column ends with “@gmail.com "

# COMMAND ----------

from pyspark.sql.functions import col
data = [
    (1, "Asdfghnbvc@gmail.com"),
    (2, "Ghfdvbg@hotmail.com"),
    (3, "Gfdc345_ds@gmail/hotmail.com")
]
columns = ["ID", "EMAIL"]
df = spark.createDataFrame(data, columns)
display(df)

# COMMAND ----------

df_gmail = df.filter(col("EMAIL").rlike(".*@gmail\.com$"))
df_gmail.show()

# COMMAND ----------

# MAGIC  
# MAGIC %md
# MAGIC 6) import the given dataframe to add a column “PRICE” with random values between 0 and 1 referencing to the below table. 

# COMMAND ----------

data_mat = [
    (1, "MAT0101KGF2"),
    (2, "MAT0101KGF3")
]
columns_mat = ["ID", "Mat_NAME"]
df_mat = spark.createDataFrame(data_mat, columns_mat)
display(df_mat)

# COMMAND ----------

from pyspark.sql import functions as F

df_with_price = df_mat.withColumn("PRICE", F.rand())
df_with_price.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 8) From the below 2 datasets Employees and Department, write a spark query to perform left join to get all employees and their departments, after joining filter records on the IT department. 

# COMMAND ----------

employee = [
    (1, 'XYZ', 'ABC', 101),
    (2, 'KJF', 'HJK', 102),
    (3, 'OII', 'TYUI', 103),
    (4, 'MNNN', 'FGH', 104)
]


department = [
    (101, 'HR'),
    (102, 'IT'),
    (103, 'Finance'),
    (105, 'Marketing')
]

employee_columns = ['emp_id', 'first_name', 'last_name', 'department_id']
department_columns = ['department_id', 'department_name']

employee_df = spark.createDataFrame(employee, employee_columns)
department_df = spark.createDataFrame(department, department_columns)
display(employee_df)


# COMMAND ----------

emp_dep_joined_df = employee_df.join(department_df,employee_df.department_id ==department_df.department_id, 'left')

it_df = emp_dep_joined_df.filter(col('department_name') == 'IT')

display(it_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 10) Analyzing E-commerce Sales Data: 
# MAGIC You're working for an e-commerce company, and they've provided you with a dataset containing information about their sales. Your task is to perform various data transformations using PySpark to generate insights. 
# MAGIC 1. Load the dataset into a PySpark DataFrame. 
# MAGIC 2. Calculate the total revenue for each order. 
# MAGIC 3. Find the top-selling products (by total quantity sold) in the dataset. 
# MAGIC 4. Calculate the average quantity and price per order. 
# MAGIC 5. Determine the total revenue for each customer. 
# MAGIC 6. Identify the date with the highest total revenue. 
# MAGIC
# MAGIC  

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
df_schema = StructType([
    StructField("Order_id", IntegerType(), True),
    StructField("Customer_id", IntegerType(), True),
    StructField("Order_date", DateType(), True),
    StructField("Product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", IntegerType(), True)
])
data = [
    (1, 101, '2023-07-01', 'A', 2, 10),
    (2, 102, '2023-07-02', 'A', 3, 15),
    (3, 101, '2023-07-02', 'A', 1, 10),
    (4, 103, '2023-07-02', 'C', 2, 30),
    (5, 102, '2023-07-03', 'A', 1, 10)
]

sales_df = spark.createDataFrame(data,df_schema)

sales_df.show()

# COMMAND ----------

from pyspark.sql.functions import col

sales_revenue_df = sales_df.withColumn("total_revenue", col("quantity") * col("price"))

sales_revenue_df.show()

# COMMAND ----------

top_products_selling_df = 
sales_df.groupBy("Product_id").agg(sum("quantity").alias("total_quantity_sold"))
.orderBy(col("total_quantity_sold").desc())
top_products_selling_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC MCQ
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 1. What is the role of the Driver in Apache Spark architecture?( c) 
# MAGIC
# MAGIC a) It schedules tasks and manages worker nodes. 
# MAGIC b) It runs on worker nodes to execute tasks. 
# MAGIC c) It manages the SparkContext and DAG of tasks. 
# MAGIC d) It stores the intermediate data during shuffle operations. 
# MAGIC
# MAGIC  
# MAGIC
# MAGIC 2. What does a Resilient Distributed Dataset (RDD) represent in Spark?(b) 
# MAGIC
# MAGIC a) A mutable collection of data distributed across a cluster. 
# MAGIC
# MAGIC b) An immutable distributed collection of objects that can be processed in parallel. 
# MAGIC
# MAGIC c) A replicated dataset that is stored permanently in HDFS. 
# MAGIC
# MAGIC d) A cached version of input data for faster execution. 
# MAGIC
# MAGIC  
# MAGIC
# MAGIC 3. In the Spark execution model, what happens during a "shuffle"?(b) 
# MAGIC
# MAGIC a) Data is broadcasted to all executors for computation. 
# MAGIC
# MAGIC b) Data is redistributed across partitions to group or aggregate it. 
# MAGIC
# MAGIC c) Data is cached in memory to speed up subsequent stages. 
# MAGIC
# MAGIC d) A DAG is broken down into individual tasks for execution. 
# MAGIC
# MAGIC  
# MAGIC
# MAGIC 4. What is the role of the "Task Scheduler" in Spark?(b) 
# MAGIC
# MAGIC a) It schedules jobs to be executed by the Driver. 
# MAGIC
# MAGIC b) It schedules tasks across worker nodes based on DAG stages. 
# MAGIC
# MAGIC c) It optimizes query plans for efficient execution. 
# MAGIC
# MAGIC d) It manages data replication across worker nodes. 
# MAGIC
# MAGIC  
# MAGIC
# MAGIC 5. Which of the following is NOT a feature of Spark's Directed Acyclic Graph (DAG)?(d ) 
# MAGIC
# MAGIC a) It allows fault-tolerant lineage tracking. 
# MAGIC
# MAGIC b) It supports cyclic dependencies between transformations. 
# MAGIC
# MAGIC c) It represents transformations and actions in a job. 
# MAGIC
# MAGIC d) It is constructed when actions are triggered. 
# MAGIC
# MAGIC  
# MAGIC
# MAGIC 6. What is the main function of an Executor in Spark?(c) 
# MAGIC
# MAGIC a) It coordinates tasks across the cluster. 
# MAGIC
# MAGIC b) It translates user code into task execution plans. 
# MAGIC
# MAGIC c) It runs tasks and stores intermediate results. 
# MAGIC
# MAGIC d) It schedules jobs for SparkContext. 
# MAGIC
# MAGIC  
# MAGIC
# MAGIC 7. How does Spark ensure fault tolerance for RDDs?(c) 
# MAGIC
# MAGIC a) By replicating data across multiple worker nodes. 
# MAGIC
# MAGIC b) By persisting data to disk after each stage. 
# MAGIC
# MAGIC c) By maintaining lineage information to recompute lost partitions. 
# MAGIC
# MAGIC d) By caching data in memory to avoid data loss. 
# MAGIC
# MAGIC  
# MAGIC 8. What is the significance of the SparkContext in Spark's architecture?(a) 
# MAGIC
# MAGIC a) It represents the entry point of a Spark application. 
# MAGIC
# MAGIC b) It directly schedules tasks on worker nodes. 
# MAGIC
# MAGIC c) It persists RDDs into memory for faster execution. 
# MAGIC
# MAGIC d) It optimizes the execution plan for SQL queries. 
# MAGIC
# MAGIC  
# MAGIC
# MAGIC 9. Which component of Spark is responsible for breaking down a job into stages?(c) 
# MAGIC
# MAGIC a) Catalyst Optimizer 
# MAGIC
# MAGIC b) Task Scheduler 
# MAGIC
# MAGIC c) DAG Scheduler 
# MAGIC
# MAGIC d) Spark Driver 
# MAGIC
# MAGIC  
# MAGIC
# MAGIC 10. What is the purpose of Spark's "lazy evaluation" mechanism?(d) 
# MAGIC
# MAGIC a) To avoid executing transformations until an action is triggered. 
# MAGIC
# MAGIC b) To cache data in memory for faster computation. 
# MAGIC
# MAGIC c) To execute jobs in a distributed manner across nodes. 
# MAGIC
# MAGIC d) To optimize query execution plans using the Catalyst Optimizer. 
# MAGIC
# MAGIC  
# MAGIC
# MAGIC 11. Which of the following transformations in Spark triggers a shuffle?(c) 
# MAGIC
# MAGIC a) map 
# MAGIC
# MAGIC b) filter 
# MAGIC
# MAGIC c) reduceByKey 
# MAGIC
# MAGIC d) flatMap 
# MAGIC
# MAGIC  
# MAGIC
# MAGIC 12. What happens when a task fails in Spark?(b) 
# MAGIC
# MAGIC a) The entire job is aborted. 
# MAGIC
# MAGIC b) Spark retries the failed task on another executor. 
# MAGIC
# MAGIC c) Spark recomputes all the RDDs from the beginning. 
# MAGIC
# MAGIC d) The Driver restarts the application automatically. 
# MAGIC
# MAGIC  
# MAGIC
# MAGIC 13. What does the term "stage" refer to in Spark?(a) 
# MAGIC
# MAGIC a) A collection of transformations that can be executed without shuffle. 
# MAGIC
# MAGIC b) A single task executed on a worker node. 
# MAGIC
# MAGIC c) The final output of a Spark job. 
# MAGIC
# MAGIC d) A cached version of an RDD in memory. 
# MAGIC
# MAGIC  
# MAGIC
# MAGIC 14. What is "speculative execution" in Apache Spark?(c) 
# MAGIC
# MAGIC a) Executing tasks in a speculative order for optimization. 
# MAGIC
# MAGIC b) Running slow tasks on additional nodes to avoid stragglers. 
# MAGIC
# MAGIC c) Caching frequently used data to speed up execution. 
# MAGIC
# MAGIC d) Splitting large RDDs into smaller partitions for faster execution. 
# MAGIC
# MAGIC  
# MAGIC
# MAGIC 15. How does Spark SQL improve query execution performance?(b) 
# MAGIC
# MAGIC a) By using the Resilient Distributed Dataset (RDD). 
# MAGIC
# MAGIC b) By leveraging the Catalyst Optimizer for query optimization. 
# MAGIC
# MAGIC c) By executing queries using the Hadoop MapReduce model. 
# MAGIC
# MAGIC d) By caching all intermediate results in memory. 
# MAGIC