# Databricks notebook source
sales_data = [
          ('A', '2021-01-01', '1'),
          ('A', '2021-01-01', '2'),
          ('A', '2021-01-07', '2'),
          ('A', '2021-01-10', '3'),
          ('A', '2021-01-11', '3'),
          ('A', '2021-01-11', '3'),
          ('B', '2021-01-01', '2'),
          ('B', '2021-01-02', '2'),
          ('B', '2021-01-04', '1'),
          ('B', '2021-01-11', '1'),
          ('B', '2021-01-16', '3'),
          ('B', '2021-02-01', '3'),
          ('C', '2021-01-01', '3'),
          ('C', '2021-01-01', '1'),
          ('C', '2021-01-07', '3')]
# cols
sales_cols= ["customer_id", "order_date", "product_id"]
sales_df = spark.createDataFrame(data = sales_data, schema = sales_cols)
# view the data
sales_df.show()

# COMMAND ----------

menu_data = [ ('1', 'palak_paneer', 100),
              ('2', 'chicken_tikka', 150),
              ('3', 'jeera_rice', 120),
              ('4', 'kheer', 110),
              ('5', 'vada_pav', 80),
              ('6', 'paneer_tikka', 180)]
# cols
menu_cols = ['product_id', 'product_name', 'price']
# create the menu dataframe
menu_df = spark.createDataFrame(data = menu_data, schema = menu_cols)
# view the data
menu_df.show()

# COMMAND ----------

members_data = [ ('A', '2021-01-07'),
                 ('B', '2021-01-09')]
# cols
members_cols = ["customer_id", "join_date"]
# create the member's dataframe
members_df = spark.createDataFrame(data = members_data, schema = members_cols)
# view the data
members_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select sales_df.customer_id,
# MAGIC sales_df.product_id,
# MAGIC menu_df.product_name,
# MAGIC sum(menu_df.price) 
# MAGIC from sales_df
# MAGIC inner join 
# MAGIC menu_df 
# MAGIC on sales_df.product_id = menu_df.product_id
# MAGIC group by sales_df.customer_id

# COMMAND ----------

total_spent_df=(sales_df.join(menu_df,'product_id')
                        .groupBy('customer_id').agg({'price':'sum'})
                        .withColumnRenamed('sum(price)','total_spent_amounts')
                        .orderBy('customer_id'))
total_spent_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC How many days has each customer visited the restaurant?

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct
customer_visited_df = sales_df.groupby('customer_id') \
    .agg(countDistinct('order_date')) \
    .orderBy('customer_id')\
    .withColumnRenamed('count(DISTINCT order_date)', 'count_visited')
customer_visited_df.display()

# COMMAND ----------

customer_visited_df=(sales_df.join(members_df,'customer_id')
                      .groupBy('customer_id').agg({'order_date':'count'})
                      .withColumnRenamed('count(order_date)','count_visited')
                      .orderBy('customer_id'))
customer_visited_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC What was each customer’s first item from the menu

# COMMAND ----------

from pyspark.sql.functions import window, dense_rank
from pyspark.sql.window import Window

windowSpec = Window.partitionBy("customer_id").orderBy("order_date")
items_purchased_df = (
    sales_df.withColumn("dense_rank", dense_rank().over(windowSpec))
    .filter("dense_rank == 1")
    .join(menu_df, 'product_id')
    .select('customer_id', 'product_name')
    .orderBy('customer_id')
)

display(items_purchased_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Find out the most purchased item from the menu and how many times the customers purchased it.

# COMMAND ----------

from pyspark.sql.functions import col
most_purchased_product = (
    sales_df.join(menu_df, 'product_id')
    .groupBy('product_name')
    .agg({'product_name': 'count'})
    .withColumnRenamed('count(product_name)', 'count_purchases')
    .orderBy(col('count(product_name)').desc())
    .limit(1)
)

display(most_purchased_product)

# COMMAND ----------

# MAGIC %md
# MAGIC Which item was the most popular for each customer?

# COMMAND ----------

from pyspark.sql.functions import window, dense_rank,col, count
from pyspark.sql.window import Window  

popular_item_df = (
    sales_df.join(menu_df, 'product_id')
    .groupBy('customer_id', 'product_name')
    .agg(count('product_id').alias('count_orders'))
    .withColumn(
        'dense_rank', 
        dense_rank().over(
            Window.partitionBy("customer_id").orderBy(col("count_orders").desc())
        )
    )
    .filter('dense_rank=1')
    .drop('dense_rank')
)

display(popular_item_df)

# COMMAND ----------

from pyspark.sql.functions import window, dense_rank
from pyspark.sql.window import Window   
windowSpec = Window.partitionBy("customer_id").orderBy("product_name")
most_popular_product = (sales_df.join(menu_df, 'product_id')
                        .groupBy('customer_id', 'product_name')
                        .agg({'product_name': 'count'})
                        .withColumn('dense_rank', dense_rank().over(windowSpec))
                        .filter("dense_rank == 1")
                        .select('customer_id', 'product_name')
                        .orderBy('customer_id'))
most_popular_product.display()
#wrong Ans


# COMMAND ----------

# MAGIC %md
# MAGIC Which item was ordered first by the customer after becoming a restaurant member?

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank

windowSpec = Window.partitionBy("customer_id").orderBy("order_date")

items_after_member_df = (
    sales_df.join(members_df, 'customer_id')
    .filter(sales_df.order_date >= members_df.join_date)
    .withColumn('dense_rank', dense_rank().over(windowSpec))
    .filter('dense_rank = 1')
    .join(menu_df, 'product_id')
    .select('customer_id', 'order_date', 'product_name')
)

display(items_after_member_df)

# COMMAND ----------

first_ordered_df = (
    sales_df.join(members_df, 'customer_id')
    .groupBy('order_date', 'customer_id')
    .orderBy('customer_id')
    .limit(1)
)
#wrong Ans
display(first_ordered_df)

# COMMAND ----------

# MAGIC %md
# MAGIC  Which item was purchased before the customer became a member

# COMMAND ----------


from pyspark.sql.window import Window
from pyspark.sql.functions import col, dense_rank

windowSpec = Window.partitionBy("customer_id").orderBy(col("order_date").desc())

items_before_member_df = (
    sales_df.join(members_df, 'customer_id')
    .filter(sales_df.order_date < members_df.join_date)
    .withColumn('dense_rank', dense_rank().over(windowSpec))
    .filter('dense_rank = 1')
    .join(menu_df, 'product_id')
    .select('customer_id', 'order_date', 'product_name')
)

display(items_before_member_df)

# COMMAND ----------

# MAGIC %md
# MAGIC What is the total items and amount spent for each member before they became a member?

# COMMAND ----------

from pyspark.sql.functions import countDistinct, sum

total_items_spent_df = (
    sales_df.join(menu_df, 'product_id')
    .join(members_df, 'customer_id')
    .filter(sales_df.order_date < members_df.join_date)
    .groupBy('customer_id')
    .agg(countDistinct('product_id').alias('item_counts'),
         sum(menu_df.price).alias('total_amount'))
    .orderBy('customer_id')
)

display(total_items_spent_df)

# COMMAND ----------

# MAGIC %md
# MAGIC If each rupee spent equates to 10 points and item ‘jeera_rice’ has a 2x points multiplier, find out how many points each customer would have.

# COMMAND ----------

from pyspark.sql.functions import when

earned_points_df = sales_df.join(menu_df, 'product_id') \
    .withColumn('points', when(col('product_id') == 3, col('price')*20)
    .otherwise(col('price')*10)) \
    .groupBy('customer_id') \
    .agg(sum('points').alias('rewards_points'))

display(earned_points_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Create the complete table with all data and columns like customer_id, order_date, product_name, price, and member(Y/N).

# COMMAND ----------

all_data_df = (sales_df.join(menu_df, 'product_id', 'left')
                      .join(members_df, 'customer_id', 'left')
                      .withColumn('member', when(col('order_date') < col('join_date'), 'N')
                                             .when(col('order_date') >= col('join_date'), 'Y')
                                             .otherwise('N'))
                      .drop('product_id', 'product_name', 'join_date'))

display(all_data_df)

# COMMAND ----------

# MAGIC %md
# MAGIC We also require further information about the ranking of customer products. The owner does not need the ranking for non-member purchases, so he expects null ranking values for the records when customers still need to be part of the membership program.

# COMMAND ----------

from pyspark.sql.functions import col, when
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

ranking_final_df = sales_df.join(menu_df, 'product_id', 'left') \
    .join(members_df, 'customer_id', 'left') \
    .withColumn('is_member', when(col('order_date') < col('join_date'), 'N')
                .when(col('order_date') >= col('join_date'), 'Y')
                .otherwise('N')) \
    .withColumn('rank', when(col('is_member') == 'N', None)
                .when(col('is_member') == 'Y', rank().over(Window.partitionBy('customer_id', 'is_member')
                                                           .orderBy('order_date')))
                .otherwise(0))

display(ranking_final_df)