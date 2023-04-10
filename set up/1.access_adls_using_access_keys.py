# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

formula1_account_key = dbutils.secrets.get(scope= 'dsformula1-scope', key = 'dsformula1dl-account-key')

# COMMAND ----------

# spark.conf.set(
#     "fs.azure.account.key.dsformula1.dfs.core.windows.net",
#     "/WbHoH7btJugUy+aBSbahO73adGPPcrTDts5pQaS7q3VscJzm9+nWobsKskRKV6EkLpzcriUV28Q+ASt4rd/gg==")
spark.conf.set(
    "fs.azure.account.key.dsformula1.dfs.core.windows.net",
    formula1_account_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dsformula1.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@dsformula1.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

sas = "sp=rl&st=2023-03-23T10:41:19Z&se=2023-03-23T18:41:19Z&spr=https&sv=2021-12-02&sr=c&sig=VeotJ2SFTzozXN23gmT2Y5NIDQUknyR82sK6LluOmwA%3D"
