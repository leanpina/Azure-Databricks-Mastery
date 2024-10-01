# Databricks notebook source

spark.conf.set("fs.azure.account.auth.type.deltadbstgpina.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.deltadbstgpina.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.deltadbstgpina.dfs.core.windows.net", "39113859-cd89-41ee-af19-bedb601eac67")
spark.conf.set("fs.azure.account.oauth2.client.secret.deltadbstgpina.dfs.core.windows.net", "U6D8Q~EI-8UPcYENtNQ9GFpKRqS~81_U8oBzXcNX")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.deltadbstgpina.dfs.core.windows.net", "https://login.microsoftonline.com/63c8a3d9-530b-4409-afe8-69c29700ddff/oauth2/token")

# COMMAND ----------

source = "abfss://test@deltadbstgpina.dfs.core.windows.net/"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType,DateType,FloatType,DoubleType

schema1 = StructType([
    StructField('Education_Level',StringType()),
    StructField('Line_Number',IntegerType()),
    StructField('Employed',IntegerType()),
    StructField('Unemployed',IntegerType()),
    StructField('Industry',StringType()),
    StructField('Gender',StringType()),
    StructField('Date_Inserted',StringType()),
    StructField('dense_rank',IntegerType())
])

# COMMAND ----------

df = (spark.read.format("csv")
            .option("header", "true")
            .schema(schema1)
            .load(f"{source}/files/*.csv")
    )

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing Parquet format

# COMMAND ----------

(df.write.format('parquet')
    .mode('overwrite')
    .save(f'{source}/ParquetFolder'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading Parquet File

# COMMAND ----------

df_parquet = (spark.read.format('parquet')
            .load(f'{source}/ParquetFolder/'))

# COMMAND ----------

df_parquet.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Atualização não é suportada em ADLS

# COMMAND ----------

df_parquet.createOrReplaceTempView('ParquetView')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC UPDATE ParquetView 
# MAGIC SET Education_Level = 'School'
# MAGIC WHERE Education_Level = 'High School'

# COMMAND ----------

spark.sql("""   UPDATE ParquetView SET Education_Level = 'School' WHERE Education_Level = 'High School'  """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter and Overwrite

# COMMAND ----------

df_parquet = df_parquet.filter("Education_level == 'High School'")

# COMMAND ----------

(df_parquet.write.format('parquet')
    .mode('overwrite')
    .save(f'{source}/Temp/'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading parquet file from Temp folder

# COMMAND ----------

df_temp = (spark.read.format('parquet')
                    .load(f'{source}/Temp/'))

# COMMAND ----------

display(df_temp)

# COMMAND ----------

(df_temp.write.format('parquet')
    .mode('overwrite')
    .save(f'{source}/ParquetFolder/'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading the overwritten file

# COMMAND ----------

df_parquet_ov = (spark.read.format('parquet')
            .load(f'{source}/ParquetFolder/'))

# COMMAND ----------

display(df_parquet_ov)

# COMMAND ----------

(df.write.format('delta')
    .mode('overwrite')
    .save(f'{source}/delta/'))
