# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest circuts.csv file

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

# to see the mounted containers
display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC using the fs magic command to see the list of files inside the raw container 

# COMMAND ----------

# MAGIC 
# MAGIC %fs
# MAGIC ls /mnt/dsformula1/raw

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading data from csv

# COMMAND ----------

#circuits_df = spark.read.csv("dbfs:/mnt/dsformula1/raw/circuits.csv")
circuits_df = spark.read.csv(f"{raw_folder_path}/circuits.csv")


# COMMAND ----------

# printing the type of cirtuite_df
type(circuits_df)

# COMMAND ----------

# by default the dataframe read API imagine that the data does not have header
# the show method shows the n first row of the dataframe
circuits_df.show(n=10)

# COMMAND ----------

# to specify the first row as header we use the option 
circuits_df = spark.read.option("header", True).csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# to see the schema of the data
circuits_df.printSchema()

# COMMAND ----------

# to see the some statictics of the data
circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fixing the data type of columns
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

circuits_df = spark.read.option("header", True) \
.schema(circuits_schema).csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select columns
# MAGIC #### there are four different ways to select columns
# MAGIC ##### except the first one, the others are more flexible

# COMMAND ----------


circuits_selected_df = circuits_df.select('circuteId', 'circuitRef', 'name', 'location', 'country', 'lat', 'lng', 'alt')

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df['circuteId'], circuits_df['circuitRef'], circuits_df['name'], circuits_df['location'], circuits_df['country'], circuits_df['lat'], circuits_df['lng'], circuits_df['alt'])

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
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

display(cirtuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add ingestion date to the dataframe

# COMMAND ----------

circuit_final_df = add_ingestion_date(cirtuits_renamed_df)

# COMMAND ----------

display(circuit_final_df)

# COMMAND ----------

# in case we want to add a column with literal value, we wrape the value with lit function
circuit_final_df_1 = cirtuits_renamed_df.withColumn("env", lit("Production"))

# COMMAND ----------

display(circuit_final_df_1)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Write data to datalake as parquet

# COMMAND ----------

# MAGIC %md
# MAGIC to make the notebook rerunable we add the overwrite mode too

# COMMAND ----------

# write the dataframe (processed container) into data lake in parquet format
# circuit_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# create circuits table inside the f1_processed database in the parquet format
circuit_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %md
# MAGIC #### check if the writing is done

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/dsformula1/processed/circuits

# COMMAND ----------

df = spark.read.parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit("Succes")
