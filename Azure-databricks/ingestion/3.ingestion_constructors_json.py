# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest Constructor_json file

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

from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 1 - Read the data

# COMMAND ----------

constructor_schema = StructType([
    StructField("constructorId", IntegerType(), False),
    StructField("constructorRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

constructor_df = spark.read.schema(constructor_schema).json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename and add the ingestion date

# COMMAND ----------

constructor_with_column_df = constructor_df.withColumnRenamed("constructorId", "constructor_id") \
                                            .withColumnRenamed("constructorRef", "constructor_ref") \
                                            .withColumn("data_source", lit(v_data_source)) \
                                            .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

constructor_final_df = constructor_with_column_df.drop("url")

# COMMAND ----------

constructor_final_df = add_ingestion_date(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write the data into Data lake as parquet

# COMMAND ----------

# write the dataframe (processed container) into data lake in parquet format
# constructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")

# create constructors table inside the f1_processed database in the parquet format
# constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# using data lakehouse architecture, and the file format is delta
constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Succes")
