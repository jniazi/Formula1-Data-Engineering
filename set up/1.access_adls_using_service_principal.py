# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks notebook source 
# MAGIC ### Access Azure Data Lake using Service Principal
# MAGIC #### Steps to follow
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate a secret/ password for the Application
# MAGIC 3. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor' to the Data Lake. 

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'dsformula1-scope', key = 'client-id')
tenant_id = dbutils.secrets.get(scope = 'dsformula1-scope', key = 'tenant-id')
client_secret = dbutils.secrets.get(scope = 'dsformula1-scope', key = 'client-secret')


# COMMAND ----------

# service_credential = dbutils.secrets.get(scope="<scope>",key="<service-credential-key>")

spark.conf.set("fs.azure.account.auth.type.dsformula1.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.dsformula1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.dsformula1.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.dsformula1.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.dsformula1.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dsformula1.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@dsformula1.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


