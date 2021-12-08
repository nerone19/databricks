# Databricks notebook source
dbutils.secrets.help()


# COMMAND ----------

dbutils.secrets.list('formula19-scope')

# COMMAND ----------

dbutils.secrets.get(scope="formula19-scope", key="databricks-app-client-id")

# COMMAND ----------

storage_account_name = "formula19"
client_id = dbutils.secrets.get(scope="formula19-scope", key="databricks-app-client-id")
tenant_id = dbutils.secrets.get(scope="formula19-scope", key="databricks-app-tenent-id")
client_secret = dbutils.secrets.get(scope="formula19-scope", key="databricks-app-client-secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{client_id}",
          "fs.azure.account.oauth2.client.secret": f"{client_secret}",
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

def mount_adls(container_name):
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)

# COMMAND ----------

dbutils.fs.unmount("/mnt/formula19/processed")
dbutils.fs.unmount("/mnt/formula19/raw")

# COMMAND ----------

mount_adls("raw")
mount_adls("processed")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula19/processed")
dbutils.fs.ls("/mnt/formula19/raw")
dbutils.fs.ls("/mnt/formula19/presentation")

# COMMAND ----------

mount_adls("presentation")

# COMMAND ----------

mount_adls("demo")

# COMMAND ----------

