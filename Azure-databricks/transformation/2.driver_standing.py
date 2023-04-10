# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col, desc, rank
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Find race years for which the data is to be reprocessed
# MAGIC when new data comes in, we have to recalculate the analysis for the year that data belongs to

# COMMAND ----------

race_result_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_result") \
.filter(f"file_date ='{v_file_date}'")

# COMMAND ----------

# get the list of years in the new data
race_year_list = df_column_to_list(race_result_df, "race_year")

# COMMAND ----------

race_result_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_result") \
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

driver_standing_df = race_result_df.groupBy("race_year", "driver_name", "driver_nationality") \
                                    .agg(sum("points").alias("total_points"),
                                        count(when(col("position") == 1, True)).alias("wins")) # if driver position is 1 then he won the race

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standing_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------


#display(final_df.filter("race_year == 2020").orderBy(desc("total_points")))

# COMMAND ----------

# write the dataframe (presentation container) into data lake in parquet format
# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standing")

# create driver_standing table inside the f1_presentation database in the parquet format
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standing")

# COMMAND ----------

# overwrite_partition(final_df, "f1_presentation", "driver_standing", "race_year")

# COMMAND ----------

merge_condition = "tgt.race_year = src.race_year and tgt.driver_name = src.driver_name"
merge_delta_data(final_df, "f1_presentation", "driver_standing", presentation_folder_path, merge_condition, "race_year")

# COMMAND ----------


