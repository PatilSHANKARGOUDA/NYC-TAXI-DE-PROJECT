# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Access

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.nyctaxidatashankardl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.nyctaxidatashankardl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.nyctaxidatashankardl.dfs.core.windows.net", "<Application ID>")
spark.conf.set("fs.azure.account.oauth2.client.secret.nyctaxidatashankardl.dfs.core.windows.net", "service_principle_secrete")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.nyctaxidatashankardl.dfs.core.windows.net", "https://login.microsoftonline.com/tenent_ID/oauth2/token")


# COMMAND ----------

dbutils.fs.ls("abfss://bronze@nyctaxidatashankardl.dfs.core.windows.net/raw//")

# COMMAND ----------

# MAGIC %md
# MAGIC **Read data**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_trip_type = spark.read.format('csv')\
                    .option('inferSchema', True)\
                    .option('header', True)\
                    .load("abfss://bronze@nyctaxidatashankardl.dfs.core.windows.net/trip_type")

# COMMAND ----------

df_trip_type.display()

# COMMAND ----------

df_trip_zone = spark.read.format('csv')\
                    .option('InferSchema',True)\
                    .option('header',True)\
                    .load('abfss://bronze@nyctaxidatashankardl.dfs.core.windows.net/trip_zone')

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Read all trip data using recursiveFileLookup**

# COMMAND ----------

my_schema = '''
    VendorID BIGINT,
    lpep_pickup_datetime TIMESTAMP,
    lpep_dropoff_datetime TIMESTAMP,
    store_and_fwd_flag STRING,
    RatecodeID BIGINT,
    PULocationID BIGINT,
    DOLocationID BIGINT,
    passenger_count BIGINT,
    trip_distance DOUBLE,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    ehail_fee DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    payment_type BIGINT,
    trip_type BIGINT,
    congestion_surcharge DOUBLE
'''

df_trip_data = spark.read.format('parquet')\
                    .schema(my_schema)\
                    .option('header', True)\
                    .option('recursiveFileLookup', True)\
                    .load('abfss://bronze@nyctaxidatashankardl.dfs.core.windows.net/raw/trip-data/')

# COMMAND ----------

df_trip_data.display()

# COMMAND ----------

df_trip_data.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip type**

# COMMAND ----------

df_trip_type = df_trip_type.withColumnRenamed('description', 'trip_description')


# COMMAND ----------

df_trip_type.write.format('parquet')\
                    .mode('overwrite')\
                    .option('path', 'abfss://silver@nyctaxidatashankardl.dfs.core.windows.net/trip_type/')\
                    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip zone**

# COMMAND ----------

df_trip_zone = df_trip_zone.withColumn('Zone1', split(col('Zone'), '/')[0])\
            .withColumn('Zone2', split(col('Zone'), '/')[1])

# COMMAND ----------

df_trip_zone = df_trip_zone.drop('Zone')

# COMMAND ----------

df_trip_zone.write.format('parquet')\
            .mode('overwrite')\
            .option('path', 'abfss://silver@nyctaxidatashankardl.dfs.core.windows.net/trip_zone/')\
            .save()


# COMMAND ----------

# MAGIC %md
# MAGIC **Trip data**

# COMMAND ----------

df_trip_data = df_trip_data.withColumn('trip_date', to_date('lpep_pickup_datetime'))\
                            .withColumn('trip_year', year('lpep_pickup_datetime'))\
                            .withColumn('trip_month', month('lpep_pickup_datetime'))


# COMMAND ----------

df_trip_data = df_trip_data.select('VendorID', 'PULocationID', 'DOLocationID', 'trip_date', 'trip_year', 'trip_month')

# COMMAND ----------

df_trip_data.write.format('parquet')\
            .mode('overwrite')\
            .option('path', 'abfss://silver@nyctaxidatashankardl.dfs.core.windows.net/trip_data=2023/')\
            .save()
