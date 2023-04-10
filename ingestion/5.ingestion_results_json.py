# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest Result_json file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 1 - Read the data

# COMMAND ----------

result_schema = StructType([
        StructField("resultId", IntegerType(), True),
        StructField("raceId", IntegerType(), True),
        StructField("driverId", IntegerType(), True),
        StructField("constructorId", IntegerType(), True),
        StructField("number", IntegerType(), True),
        StructField("grid", IntegerType(), True),
        StructField("position", IntegerType(), True),
        StructField("positionText", StringType(), True),
        StructField("positionOrder", IntegerType(), True),
        StructField("points", FloatType(), True),
        StructField("laps", IntegerType(), True),
        StructField("time", StringType(), True),
        StructField("milliseconds", IntegerType(), True),
        StructField("fastestLap", IntegerType(), True),
        StructField("rank", IntegerType(), True),
        StructField("fastestLapTime", StringType(), True),
        StructField("fastestLapSpeed", StringType(), True),
        StructField("statusId", IntegerType(), True),
])

# COMMAND ----------

result_df = spark.read.schema(result_schema).json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename and add the ingestion date

# COMMAND ----------

result_with_column_df = result_df.withColumnRenamed("resultId", "result_id") \
                                            .withColumnRenamed("raceId", "race_id") \
                                            .withColumnRenamed("driverId", "driver_id") \
                                            .withColumnRenamed("constructorId", "constructor_id") \
                                            .withColumnRenamed("positionText", "position_text") \
                                            .withColumnRenamed("positionOrder", "position_order") \
                                            .withColumnRenamed("fastestLap", "fastest_lap") \
                                            .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                            .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                            .withColumn("data_source", lit(v_data_source)) \
                                            .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

result_final_df = result_with_column_df.drop("statusId")

# COMMAND ----------

result_final_df = add_ingestion_date(result_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### De-dupe the dataframe

# COMMAND ----------

result_deduped_df = result_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write the data into Data lake as parquet
# MAGIC ##### Data lake only support INSERT and LOAD data manipulation (not delete and upldate). To implement incremental data load, we should delete the specific partition and then add the new one. To do this there are two approaches:

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 1

# COMMAND ----------

# in this for loop we are deleting the partitions that are already exist and write it again, this is done by partition by race_id
# collect method gets the data and put it in the driver's node memory as a list
#for race_id_list in result_final_df.select("race_id").distinct().collect():
    #if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
        #spark.sql(f"alter table f1_processed.results drop if exists partition (race_id = {race_id_list.race_id})")

# COMMAND ----------

# write the dataframe (processed container) into data lake in parquet format
#result_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/results")

# create results table inside the f1_processed database in the parquet format
# result_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.results")

# create incremental data load into table inside the f1_processed database in the parquet format
#result_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 2
# MAGIC this method is more efficient because spark find the existed record and overwrite it instead of looping over all the data and deleting the data manually like in method 1

# COMMAND ----------

# overwrite_partition(result_final_df, "f1_processed", "results", "race_id")

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id and tgt.race_id = src.race_id"
merge_delta_data(result_deduped_df, "f1_processed", "results", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC ---DROP TABLE f1_processed.results

# COMMAND ----------

# first we check if the table exists, use insertInto method otherwise the saveAsTable method
# if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     result_final_df.write.mode("overwrite").insertInto("f1_processed.results")
# else:
#     result_final_df.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/results"))

# COMMAND ----------

dbutils.notebook.exit("Succes")

# COMMAND ----------


