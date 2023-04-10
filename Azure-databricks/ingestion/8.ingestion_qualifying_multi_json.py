# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest Qualifying folder which contains two multiline json files

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source= dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 1 - Read the data

# COMMAND ----------

qualifying_schema = StructType([StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), False),
                                      StructField("constructorId", IntegerType(), False),
                                      StructField("number", IntegerType(), False),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True)
                                     ])

# COMMAND ----------


qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{v_file_date}/qualifying")


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename and add the ingestion date

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
                        .withColumnRenamed("driverId", "driver_id") \
                        .withColumnRenamed("raceId", "race_id") \
                        .withColumnRenamed("constructorId", "constructor_id") \
                        .withColumn("data_source", lit(v_data_source)) \
                        .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

final_df = add_ingestion_date(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write the data into Data lake as parquet

# COMMAND ----------

# write the dataframe (processed container) into data lake in parquet format
#final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")

# create qualifying table inside the f1_processed database in the parquet format
#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id and tgt.qualify_id = src.qualify_id"
merge_delta_data(final_df, "f1_processed", "qualifying", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("Succes")

# COMMAND ----------


