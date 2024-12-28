# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Access and Reading
# MAGIC

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.nyctaxidatashankardl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.nyctaxidatashankardl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.nyctaxidatashankardl.dfs.core.windows.net", "fe24c27c-54d7-4ad5-bd62-aba427ed0253")
spark.conf.set("fs.azure.account.oauth2.client.secret.nyctaxidatashankardl.dfs.core.windows.net", "qb98Q~5sXv7o79_aA_n4EkablLvplmL-eYu2FaId")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.nyctaxidatashankardl.dfs.core.windows.net", "https://login.microsoftonline.com/ec75aa0c-4e8e-4bfa-bbf4-12c24f3bc7a4/oauth2/token")


# COMMAND ----------

silver = 'abfss://silver@nyctaxidatashankardl.dfs.core.windows.net'
gold   = 'abfss://gold@nyctaxidatashankardl.dfs.core.windows.net'

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_trip_type = spark.read.format('parquet')\
                    .option('inferSchema', 'true')\
                    .option('header', True)\
                    .load(f'{silver}/trip_type')

# COMMAND ----------

df_trip_type.display()

# COMMAND ----------

df_trip_type.write.format('delta')\
            .mode('overwrite')\
            .option('path', f'{gold}/trip_type/')\
            .save()

# COMMAND ----------

df_trip_zone = spark.read.format('parquet')\
                    .option('inferSchema', True)\
                    .option('header', True)\
                    .load(f'{silver}/trip_zone')

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

df_trip_zone.write.format('delta')\
            .mode('overwrite')\
            .option('path', f'{gold}/trip_zone/')\
            .save()

# COMMAND ----------

df_trip_data = spark.read.format('parquet')\
                    .option('inferSchema', True)\
                    .option('header', True)\
                    .load(f'{silver}/trip_data=2023')

# COMMAND ----------

df_trip_data.display()

# COMMAND ----------

df_trip_data.write.format('delta')\
            .mode('overwrite')\
            .option('path', f'{gold}/trip_data=2023')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating, Defining and granting access to EXTERNAL LOCATION for EXTERNAL TABLE**