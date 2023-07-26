# Databricks notebook source
#imports

import requests
import json

# COMMAND ----------

AzureOneEnvDoamin = "adb-8151748151718168.8.azuredatabricks.net"
#AzureOneEnvToken = ""
AzureOneEnvToken = dbutils.secrets.get(scope="kv-ontada-qa-secret", key="AzureOneEnvToken")

endpoint = "https://%s/api/2.1/jobs/run-now" % (AzureOneEnvDoamin)
headers = {'Authorization': 'Bearer %s' % AzureOneEnvToken}
job_id = dbutils.widgets.get('job_id')

# COMMAND ----------

response = requests.post(
  endpoint,
  headers = headers
 ,json = {
     "job_id": job_id
 }
)

if response.status_code == 200:
  print(response.json())
else:
  print(response.content)
  print("Error triggering the pipeline")
