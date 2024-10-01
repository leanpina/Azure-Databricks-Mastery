# Databricks notebook source
print("Hello World")

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.deltadbstgpina.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.deltadbstgpina.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.deltadbstgpina.dfs.core.windows.net", "39113859-cd89-41ee-af19-bedb601eac67")
spark.conf.set("fs.azure.account.oauth2.client.secret.deltadbstgpina.dfs.core.windows.net", "U6D8Q~EI-8UPcYENtNQ9GFpKRqS~81_U8oBzXcNX")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.deltadbstgpina.dfs.core.windows.net", "https://login.microsoftonline.com/63c8a3d9-530b-4409-afe8-69c29700ddff/oauth2/token")

# COMMAND ----------

df = (spark.read.format("csv")
                .option("header", "true")
                .load("abfss://test@deltadbstgpina.dfs.core.windows.net/sample/*.csv")
)


# COMMAND ----------

display(df)

# COMMAND ----------


