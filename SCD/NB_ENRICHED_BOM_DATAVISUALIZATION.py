# Databricks notebook source
# DBTITLE 1,Reading Enriched bom GOLD
final_result=spark.table("interns_adls.anusha.enriched_bom_GOLD")

# COMMAND ----------

# DBTITLE 1,Enriched Bom Final result
display(final_result)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Matplotlib

# COMMAND ----------

# DBTITLE 1,Libraries for visualization
import matplotlib.pyplot as pt
import seaborn as sb
import pandas as pd
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Material category percentage
final_df_pd = final_result.groupBy(final_result.MaterialCategory).count().toPandas()
#final_df_pd.display()
colors = sb.color_palette("Pastel2")[: len(final_df_pd)]
pt.figure(figsize=(13, 13))
wedges, _, texts = pt.pie(
    final_df_pd["count"],
    labels=None,
    autopct="%1.3f%%",
    startangle=45,
    colors=colors,
    pctdistance=1.08,
)
pt.title("Material Category Distribution", fontsize=14)
pt.legend(
    wedges,
    final_df_pd["MaterialCategory"],
    title="Material Category",
    loc="center left",
    bbox_to_anchor=(1, 0.5),
)

# COMMAND ----------

# DBTITLE 1,Nulls Count for Column
null_counts = final_result.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in final_result.columns])

Null_df_create= spark.createDataFrame(null_counts.toPandas().T.reset_index())
Null_df=Null_df_create.withColumnRenamed("index", "column_name").withColumnRenamed("0", "null_count")


# COMMAND ----------

# DBTITLE 1,Matplotlib Nulls Count for Columns
pdf=Null_df.toPandas()
pt.figure(figsize=(8, 8))
ax=sb.barplot(y=pdf["column_name"],x=pdf["null_count"], palette="Set3")  
ax.bar_label(ax.containers[0], fmt="%d", padding=3, fontsize=10)
ax.spines['right'].set_visible(False)
pt.xlabel("Nulls_Count", fontsize=10)
pt.ylabel("Column_name", fontsize=10)
pt.title("Column_wise Nulls_Distribution", fontsize=14)
pt.yticks(fontsize=8)  
pt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## PLOTLY

# COMMAND ----------

# DBTITLE 1,Plotly Libraries for visualization
import plotly.graph_objects as go
import plotly.express as px

# COMMAND ----------

# DBTITLE 1,plotly MaterialCategory percentage
fig = px.pie(final_df_pd, 
             names='MaterialCategory', 
             values='count', 
             title="Material Category Distribution",
             color_discrete_sequence=px.colors.sequential.Viridis)
fig.update_layout(width=600, height=600)
fig.show()

# COMMAND ----------

# DBTITLE 1,plotly MaterialCategory percentage
final_df_pd = final_result.groupBy('MaterialCategory').count().toPandas()

fig = go.Figure(data=[go.Pie(labels=final_df_pd["MaterialCategory"], 
                             values=final_df_pd["count"], 
                             marker=dict(colors=px.colors.qualitative.Pastel2),
                             textinfo='percent')])

fig.update_layout(title="Material Category Distribution", 
                  width=800, 
                  height=800)

fig.show()

# COMMAND ----------

# DBTITLE 1,Plotly Bar chart Null Count
colors = px.colors.qualitative.Set3 * (len(pdf) // len(px.colors.qualitative.Set3) + 1)
colors = colors[:len(pdf)]  
fig_b = go.Figure(go.Bar(
    x=pdf["null_count"],
    y=pdf["column_name"],
    marker=dict(color=colors), 
    orientation="h" 
))
fig_b.update_layout(title="Column_wise Nulls_Distribution", width=800, height=800)
fig_b.show()