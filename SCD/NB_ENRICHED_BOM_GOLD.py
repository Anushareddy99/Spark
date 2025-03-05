# Databricks notebook source
# DBTITLE 1,Reading delta
#reading from delta
BOM_Data = spark.table(
    "interns_adls.anusha.bom_data"
)

Supplier_Volume = spark.table(
    "interns_adls.anusha.supplier_volume"
)

SML_Table = spark.table(
    "interns_adls.anusha.sml_table"
)

Spend_Table = spark.table(
    "interns_adls.anusha.spend_table"
)



# COMMAND ----------

# DBTITLE 1,Libraries of pyspark
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step-1

# COMMAND ----------

# MAGIC %md
# MAGIC Filtering Supplier Volume and Joining Bom data and supplier volume

# COMMAND ----------

# DBTITLE 1,Filter and Join SV
#filtering supplier volume table last 3 year greater than zero,supplerId is not null and blank 
Filter_SV = Supplier_Volume.filter((Supplier_Volume.LAST_3_YR_VOL > 0) &
                                   (Supplier_Volume.SUPLR_ID.isNotNull())&
                                   (Supplier_Volume.SUPLR_ID!=''))

#joining bom and supplier volume
Join_BD_SV = BOM_Data.join(Filter_SV,
                        ( BOM_Data["ITM_MTL_NO"]==Filter_SV["MTL_NO"]) &
                        ( BOM_Data["ITM_SRC_SYS_CD"]==Filter_SV["SRC_SYS_CD"])&
                        ( BOM_Data["ITM_MTL_PLNT_CD"]==Filter_SV["MTL_PLNT_CD"]),"left")

Join_BD_SV = Join_BD_SV.withColumn("Sector", coalesce(col("Sector"), lit("INNOVATIVE MEDICINE")))\
      .drop('Timestamp',
            'SRC_SYS_CD',
            'MTL_NO',
            'MTL_DESCN',
            'MTL_PLNT_CD',
            'CURR_YR_VOL',
            'LAST_1_YR_VOL',
            'LAST_2_YR_VOL',
            '_pk_',
            'SRS_UPDT_TIME')


# COMMAND ----------

# MAGIC %md
# MAGIC ###STEP-2

# COMMAND ----------

# MAGIC %md
# MAGIC #### Level-1
# MAGIC

# COMMAND ----------

# DBTITLE 1,Filter and join SML
Filter_SML_Format=SML_Table.withColumn('LAST_USED_DATE',to_date(col("LAST_USED_DATE"),'yyyyMMdd')
                                ).drop('ERP_CD',
                                       'SUP_NM1',
                                       'SUP_ADDR_LINE_1_TXT',
                                       'SUP_CITY_NM',
                                       'SUP_RGN_NM',
                                       'SUP_PSTL_CD_NUM',
                                       'SUP_CTRY_CD',
                                       'DATA_SOURCE',
                                       'CreatedDate',
                                       'UpdatedDate',
                                       'PRMRY_MFG_LOC_FLAG',
                                       'MATL_SHRT_DESC')

#filtering SML table last three years and status is active (blank)
Filter_SML = Filter_SML_Format.filter(
    (Filter_SML_Format["LAST_USED_DATE"].between(add_months(current_date(),-36),current_date())) &
    ((Filter_SML_Format['STATUS'].isNull())|(Filter_SML_Format['STATUS']=='')))
    

#join the result dataset from Step 1 with the SML table to add manufacturing details for raw_materials.

Level_1=Join_BD_SV.join(Filter_SML,
                        (Join_BD_SV.ITM_MTL_NO == Filter_SML.MATL_NUM) &
                        (Join_BD_SV.ITM_MTL_PLNT_CD == Filter_SML.PLANT)&
                        (Join_BD_SV.SUPLR_ID == Filter_SML.SUP_NUM)&
                        (Join_BD_SV.ITM_SRC_SYS_CD == upper(Filter_SML.SRC_SYS_CD)),
                        "inner").drop(Filter_SML.MATL_NUM,
                                      Filter_SML.PLANT,
                                      Filter_SML.SUP_NUM,
                                      Filter_SML.SRC_SYS_CD)


Level_1=Level_1.withColumn("join_Level",lit("level_1_join"))#add column to identify the join of level_1_join


# COMMAND ----------

# MAGIC %md
# MAGIC #### Level-2
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,unmatched records from Level-1
Level_1_sml=Join_BD_SV.join(Filter_SML,
                        (Join_BD_SV.ITM_MTL_NO == Filter_SML.MATL_NUM) &
                        (Join_BD_SV.ITM_MTL_PLNT_CD == Filter_SML.PLANT)&
                        (Join_BD_SV.SUPLR_ID == Filter_SML.SUP_NUM)&
                        (Join_BD_SV.ITM_SRC_SYS_CD == upper(Filter_SML.SRC_SYS_CD)),"left_anti")


# COMMAND ----------

# DBTITLE 1,Filter and joining of SML
#Filtering status is X inactive
Filter_2=Filter_SML_Format.filter((SML_Table.STATUS=="X"))
          
#unmatch records with filter_2 joining
Level_2=Level_1_sml.join(Filter_2,
                        (Level_1_sml.ITM_MTL_NO == Filter_2.MATL_NUM) &
                        (Level_1_sml.ITM_MTL_PLNT_CD == Filter_2.PLANT)&
                        (Level_1_sml.SUPLR_ID == Filter_2.SUP_NUM)&
                        (Level_1_sml.ITM_SRC_SYS_CD == upper(Filter_2.SRC_SYS_CD)),"inner").drop(Filter_2.MATL_NUM,
                          Filter_2.PLANT,
                          Filter_2.SUP_NUM,
                          Filter_2.SRC_SYS_CD)
                        
Level_2=Level_2.withColumn("join_Level",lit("level_2_join"))


# COMMAND ----------

# MAGIC %md
# MAGIC #### Level-3

# COMMAND ----------

# DBTITLE 1,No Match Records
Level_3=Level_1_sml.join(Filter_2,
                        (Level_1_sml.ITM_MTL_NO == Filter_2.MATL_NUM) &
                        (Level_1_sml.ITM_MTL_PLNT_CD == Filter_2.PLANT)&
                        (Level_1_sml.SUPLR_ID == Filter_2.SUP_NUM)&
                        (Level_1_sml.ITM_SRC_SYS_CD == upper(Filter_2.SRC_SYS_CD)),"left")

Level_3 = Level_3.filter(
    ((Level_3.MATL_NUM.isNull()) | (Level_3.MATL_NUM == '')) &
    ((Level_3.PLANT.isNull()) | (Level_3.PLANT == '')) &
    ((Level_3.SUP_NUM.isNull()) | (Level_3.SUP_NUM == '')) &
    ((Filter_2.SRC_SYS_CD.isNull()) | (Filter_2.SRC_SYS_CD == '')
     ))
     
Level_3=Level_3.drop(Filter_2.MATL_NUM,
            Filter_2.PLANT,
            Filter_2.SUP_NUM,
            Filter_2.SRC_SYS_CD)
Level_3=Level_3.withColumn("join_Level",lit("NO MATCH"))


# COMMAND ----------

# DBTITLE 1,Add Join-Level Column
union=Level_1.union(Level_2).union(Level_3) #unioning all the dataframes
union.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###STEP-3

# COMMAND ----------

# DBTITLE 1,Filter and Joining Spend category

Spend_Table = Spend_Table.withColumn(
    "JnJMaterialCode",
    regexp_replace(col("JnJMaterialCode"),"^0+", "")
)
display(Spend_Table)

final_result = (union.join(
                    Spend_Table,
                    (union.ITM_MTL_NO == Spend_Table.JnJMaterialCode) &
                    (union.ITM_SRC_SYS_CD == upper(Spend_Table.SourceSystemCode)),
                    'left')
                .drop('JnJMaterialCode', 'SourceSystemCode')
                .withColumnRenamed('MaterialCategoryActual', 'MaterialCategory')
                .withColumnRenamed('MaterialSubCategoryActual', 'MaterialSubCategory'))
display(final_result)


# COMMAND ----------

# DBTITLE 1,Writing into delta final output
final_result.write.format('delta').mode("overwrite")\
          .option("overwriteSchema", "true")\
          .option('path','abfss://source@internsstorage1.dfs.core.windows.net/anusha/enriched_bom_GOLD')\
          .saveAsTable("interns_adls.anusha.enriched_bom_GOLD")

# COMMAND ----------

# DBTITLE 1,Getting notebook details Auditing
notebook_data = {
    "NotebookName": dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/')[-1],
    "UserName": dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get(),
    "OperationType":"Gold"
}
 
dbutils.notebook.exit(notebook_data)