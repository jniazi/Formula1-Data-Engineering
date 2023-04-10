# Databricks notebook source
drivers_day1_df = spark.read.option("inferSchema", True).json("/mnt/dsformula1/raw/2021-03-28/drivers.json").filter("driverId <= 10") \
    .select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

from pyspark.sql.functions import upper, current_timestamp

# COMMAND ----------


drivers_day2_df = spark.read.option("inferSchema", True).json("/mnt/dsformula1/raw/2021-03-28/drivers.json").filter("driverId between 6 and 15") \
    .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------


drivers_day3_df = spark.read.option("inferSchema", True).json("/mnt/dsformula1/raw/2021-03-28/drivers.json").filter("driverId between 1 and 5 or driverId between 16 and 20") \
    .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

# MAGIC %sql
# MAGIC create database f1_demo
# MAGIC location '/mnt/dsformula1/demo'

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists f1_demo.drivers_merge(
# MAGIC   driverId int,
# MAGIC   dob date,
# MAGIC   forename string,
# MAGIC   surname string,
# MAGIC   createdDate date,
# MAGIC   updateDate DATE
# MAGIC )
# MAGIC using DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updateDate = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (driverId, dob, forename, surname, createdDate) values (driverId, dob, forename, surname, current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updateDate = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (driverId, dob, forename, surname, createdDate) values (driverId, dob, forename, surname, current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------


