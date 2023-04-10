# Databricks notebook source
# MAGIC %md
# MAGIC ## Mount Azure Data Lake Containers for the porject

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    client_id = dbutils.secrets.get(scope = 'dsformula1-scope', key = 'client-id')
    tenant_id = dbutils.secrets.get(scope = 'dsformula1-scope', key = 'tenant-id')
    client_secret = dbutils.secrets.get(scope = 'dsformula1-scope', key = 'client-secret')
    
    # Set spark configuration
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # check if the mount exists, unmount it 
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    # mount the storage account container
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------


mount_adls('dsformula1', 'demo')

# COMMAND ----------

mount_adls('dsformula1', 'processed')

# COMMAND ----------

mount_adls('dsformula1', 'raw')

# COMMAND ----------

mount_adls('dsformula1', 'presentation')

# COMMAND ----------


