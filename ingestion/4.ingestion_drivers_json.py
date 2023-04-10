# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest Drivers_json file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, lit, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 1 - Read the data

# COMMAND ----------

name_schema = StructType([
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])

# COMMAND ----------

driver_schema = StructType([
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

driver_df = spark.read.schema(driver_schema).json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. driverId renamed to driver_id  
# MAGIC 1. driverRef renamed to driver_ref  
# MAGIC 1. ingestion date added
# MAGIC 1. name added with concatenation of forename and surname

# COMMAND ----------

driver_with_column_df = driver_df.withColumnRenamed("driverId", "driver_id") \
                                            .withColumnRenamed("driverRef", "driver_ref") \
                                            .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
                                            .withColumn("data_source", lit(v_data_source)) \
                                            .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

driver_with_column_df = add_ingestion_date(driver_with_column_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop the unwanted columns
# MAGIC 1. drop url

# COMMAND ----------

driver_final_df = driver_with_column_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write the data into Data lake as parquet

# COMMAND ----------

# write the dataframe (processed container) into data lake in parquet format
# driver_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")

# create drivers table inside the f1_processed database in the parquet format
# driver_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# using data lakehouse architecture, and the file format is delta
driver_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Succes")
