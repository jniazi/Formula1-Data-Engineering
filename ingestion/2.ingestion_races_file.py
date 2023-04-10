# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType
from pyspark.sql.functions import date_format, col, current_timestamp, lit, to_timestamp, concat

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1- Read the CSV file using the spark dataframe reader

# COMMAND ----------

# we can also use the following to make the spark to infer the schema by itself, 
# but the problem is that it creates two spark jobs 
# races_df = spark.read.csv("/mnt/dsformula1/raw/races.csv", header=True, inferSchema=True)

# COMMAND ----------

races_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True),
])
races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# races_df = races_df.withColumn("time", date_format("time", "HH:mm:ss"))

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Add ingestion date and combine date and time columns

# COMMAND ----------

races_with_combined_date_df = races_df.withColumn("race_timestamp", to_timestamp(concat(col('date'),lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
                                        .withColumn("data_source", lit(v_data_source)) \
                                        .withColumn("file_date", lit(v_file_date))
races_with_ingestion_date_df = add_ingestion_date(races_with_combined_date_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Select the required columns and rename as required

# COMMAND ----------

races_final_df = races_with_ingestion_date_df.select(col('raceId').alias("race_id"), col('year').alias("race_year"), col('round'), 
                                    col('circuitId').alias("circuit_id"), col('name'), col('race_timestamp'), col("data_source")
                                                     , col('ingestion_date'), col('file_date'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write data to datalake as parquet
# MAGIC ###### Partitioning
# MAGIC Partitioning is a technique that spark devide the data into folders based on the column we specify to read the data faster.
# MAGIC In the following we use race_year as a portition column

# COMMAND ----------


# write the dataframe (processed container) into data lake in parquet format
# races_final_df.write.mode("overwrite").partitionBy('race_year').parquet(f"{processed_folder_path}/races")

# create races table inside the f1_processed database in the parquet format
# races_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")

# using data lakehouse architecture, and the file format is delta
races_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")
