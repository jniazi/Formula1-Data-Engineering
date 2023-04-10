# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races") \
                            .withColumnRenamed("name", "race_name") \
                            .withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits").withColumnRenamed("location", "circuit_location")

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers").withColumnRenamed("name", "driver_name") \
                                                                    .withColumnRenamed("nationality", "driver_nationality") \
                                                                    .withColumnRenamed("number", "driver_number")

# COMMAND ----------

constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors").withColumnRenamed("name", "team")

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results") \
                            .filter(f"file_date = '{v_file_date}'") \
                            .withColumnRenamed("time", "race_time") \
                            .withColumnRenamed("race_id", "results_race_id") \
                            .withColumnRenamed("file_date", "results_file_date")

# COMMAND ----------

race_circuit = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
.select(races_df.race_id, races_df.race_name, races_df.race_year, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

race_result_df = results_df.join(race_circuit, race_circuit.race_id == results_df.results_race_id) \
                        .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                        .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp

# COMMAND ----------

final_df = race_result_df.select("race_id","race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality",
                                      "team", "grid", "fastest_lap", "race_time", "points", "position", "results_file_date") \
                                .withColumn("created_date", lit(current_timestamp())) \
                                .withColumnRenamed("results_file_date", "file_date")

# COMMAND ----------

#display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

# write the dataframe (presentation container) into data lake in parquet format
# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_result")

# create race_reslut table inside the f1_presentation database in the parquet format
#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_result")

# COMMAND ----------

# overwrite_partition(final_df, "f1_presentation", "race_result", "race_id")

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id and tgt.driver_name = src.driver_name"
merge_delta_data(final_df, "f1_presentation", "race_result", presentation_folder_path, merge_condition, "race_id")

# COMMAND ----------

#display(spark.read.parquet(f"{presentation_folder_path}/race_result"))

# COMMAND ----------

dbutils.notebook.exit("Succes")

# COMMAND ----------

# display(spark.read.format("delta").load(f"{presentation_folder_path}/race_result"))

# COMMAND ----------


