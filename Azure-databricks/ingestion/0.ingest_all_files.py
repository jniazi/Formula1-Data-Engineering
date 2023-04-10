# Databricks notebook source
#dbutils.notebook.run("1.ingestion_circute_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

#dbutils.notebook.run("2.ingestion_races_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

#dbutils.notebook.run("3.ingestion_constructors_json", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

#dbutils.notebook.run("4.ingestion_drivers_json", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

dbutils.notebook.run("5.ingestion_results_json", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

dbutils.notebook.run("6.ingestion_pitstops_json", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

dbutils.notebook.run("7.ingestion_lap_times_multifile_csvs", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

dbutils.notebook.run("8.ingestion_qualifying_multi_json", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})
