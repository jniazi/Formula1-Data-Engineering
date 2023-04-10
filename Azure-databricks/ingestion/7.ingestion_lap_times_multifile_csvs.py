# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest lap_times folder which contains five csv files

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 1 - Read the data

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

# we can either use the folder name to address the files or
lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# we can use wildcard to address the files
# pit_stops_df = spark.read \
# .schema(lap_times_schema) \
# .json("/mnt/dsformula1/raw/lap_times/lap_times_split*")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename and add the ingestion date

# COMMAND ----------

final_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
                        .withColumnRenamed("raceId", "race_id") \
                        .withColumn("data_source", lit(v_data_source)) \
                        .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

final_df = add_ingestion_date(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write the data into Data lake as parquet

# COMMAND ----------

# write the dataframe (processed container) into data lake in parquet format
#final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")

# create lap_times table inside the f1_processed database in the parquet format
#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

# overwrite_partition(final_df, "f1_processed", "lap_times", "race_id")

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id and tgt.driver_id = src.driver_id and tgt.lap = src.lap"
merge_delta_data(final_df, "f1_processed", "lap_times", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/lap_times"))

# COMMAND ----------

dbutils.notebook.exit("Succes")
