# Databricks notebook source
# DBTITLE 1,Internal storage of Azure
#%fs ls "abfss://source@internsstorage1.dfs.core.windows.net/anusha/SRS_USECASE/"

# COMMAND ----------

# DBTITLE 1,Installing Excel Library
# MAGIC %pip install openpyxl
# MAGIC %restart_python
# MAGIC

# COMMAND ----------

# DBTITLE 1,pandas library
import pyspark.pandas as pd

# COMMAND ----------

# DBTITLE 1,Reading CSV and Excel formats
#Reading CSV and Excel files

BOM_Data=(spark.read
            .format("csv")
            .option("header", "true")
            .option("inferschema","true")
            .load('abfss://source@internsstorage1.dfs.core.windows.net/anusha/SRS_USECASE/cbom_im_extrct.csv') )


Supplier_Volume_pd=pd.read_excel("abfss://source@internsstorage1.dfs.core.windows.net/anusha/SRS_USECASE/suplr_vol_im_new_2.xlsx",
                              engine='openpyxl',
                              sheet_name = "suplr_vol_im_new",
                              dtype = str)
Supplier_Volume=Supplier_Volume_pd.to_spark()



sml_pd = pd.read_excel(
    "abfss://source@internsstorage1.dfs.core.windows.net/anusha/SRS_USECASE/sml_im_2.xlsx",
    engine='openpyxl',
    sheet_name = "sml_im_2",
    dtype = str
)
SML_Table = sml_pd.to_spark()

Spend_Table=(spark.read
            .format("csv")
            .option("header", "true")
            .option("inferschema","true")
            .load('abfss://source@internsstorage1.dfs.core.windows.net/anusha/SRS_USECASE/spend_category.csv') )


# COMMAND ----------

# MAGIC %md
# MAGIC ##Writing into Delta Table Format

# COMMAND ----------

# DBTITLE 1,files saving in delta tables
#writing to delta files
BOM_Data.write\
    .format("delta")\
    .mode("overwrite")\
    .option("path", "abfss://source@internsstorage1.dfs.core.windows.net/anusha/SRS_USECASE/BOM_Data.delta")\
    .saveAsTable("interns_adls.anusha.BOM_Data")

Supplier_Volume.write\
    .format("delta")\
    .mode("overwrite")\
    .option("path","abfss://source@internsstorage1.dfs.core.windows.net/anusha/SRS_USECASE/suplr_vol_im_new_2.delta")\
    .saveAsTable("interns_adls.anusha.Supplier_Volume")


SML_Table.write\
    .format("delta")\
    .mode("overwrite")\
    .option("path","abfss://source@internsstorage1.dfs.core.windows.net/anusha/SRS_USECASE/SML_Table_2.delta")\
    .saveAsTable("interns_adls.anusha.SML_Table")

Spend_Table.write\
    .format("delta")\
    .mode("overwrite")\
    .option("path","abfss://source@internsstorage1.dfs.core.windows.net/anusha/SRS_USECASE/spend_category_1.delta")\
    .saveAsTable("interns_adls.anusha.Spend_Table")

# COMMAND ----------

# DBTITLE 1,Getting notebook details Auditing
notebook_data = {
    "NotebookName": dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/')[-1],
    "UserName": dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get(),
    "OperationType": "Raw"
}
 
dbutils.notebook.exit(notebook_data)
 