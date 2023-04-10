# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest circuts.csv file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Defining two parameters

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading the data and fixing the data type of columns
# MAGIC  The best practice and in production we should follow the following:
# MAGIC  define the schema and then tell the dataframe to consider it

# COMMAND ----------

# import the required methods to define the schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

# define the schema
circuits_schema = StructType(fields = [StructField("circuteId", IntegerType(), False), # column name, type, nullable
                                      StructField("circuitRef", StringType(), True),
                                      StructField("name", StringType(), True),
                                      StructField("location", StringType(), True),
                                      StructField("country", StringType(), True),
                                      StructField("lat", DoubleType(), True),
                                      StructField("lng", DoubleType(), True),
                                      StructField("alt", IntegerType(), True),
                                      StructField("url", StringType(), True)
                                      ])

# COMMAND ----------

circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select columns

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

circuits_selected_df = circuits_df.select(col('circuteId'), col('circuitRef'), col('name'), col('location'), col('country'), col('lat'), col('lng'), col('alt'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rename the columns

# COMMAND ----------

cirtuits_renamed_df = circuits_selected_df.withColumnRenamed("circuteId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add ingestion date to the dataframe

# COMMAND ----------

circuit_final_df = add_ingestion_date(cirtuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write data to data lake as parquet

# COMMAND ----------

# MAGIC %md
# MAGIC to make the notebook rerunable we add the overwrite mode too

# COMMAND ----------

# write the dataframe (processed container) into data lake in parquet format
# circuit_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# create circuits table inside the f1_processed database in the parquet format
# circuit_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# using data lakehouse architecture, and the file format is delta
circuit_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %md
# MAGIC #### check if the writing is done

# COMMAND ----------

dbutils.notebook.exit("Succes")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.circuits

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deduplicate the data

# COMMAND ----------


