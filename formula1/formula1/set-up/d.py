# Databricks notebook source
dbutils.secrets.help()


# COMMAND ----------

dbutils.secrets.list('formula-scope')

# COMMAND ----------

dbutils.secrets.get(scope="formula-scope", key="databricks-app-client-id")

# COMMAND ----------

for x in dbutils.secrets.get(scope="formula-scope", key="databricks-app-client-id"):
    print(x)

# COMMAND ----------

storage_account_name = "formula19"
client_id = "c57cc7c4-afd5-487c-a1c2-5c3e2687ae7e"
tenant_id = "33ce671e-c111-4e30-900b-e8899c5513a0"
client_secret = "GIM7Q~JfLliJRwwvQ-XEgCpzuRmUkww4R~U_Z"

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

# mount_adls("raw")
mount_adls("processed")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula19/processed")

# COMMAND ----------

