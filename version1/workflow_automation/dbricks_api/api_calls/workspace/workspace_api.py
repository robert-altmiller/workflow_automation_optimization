# Databricks notebook source
# DBTITLE 1,Databricks API 2.0 - Create Notebook
def create_workspace_notebook(dbricks_instance = None, dbricks_pat = None, notebook_path = None, language = None):
  """create workspace notebook"""
  jsondata = {"path": notebook_path, "language": language}
  response = execute_rest_api_call(post_request, get_api_config(dbricks_instance, api_topic = "workspace", api_call_type = "import"), dbricks_pat, jsondata)
  return response

# COMMAND ----------

# DBTITLE 1,Databricks API 2.0 - Make Workspace Directories
def create_workspace_directory(dbricks_instance = None, dbricks_pat = None, directory_path = None):
  """create workspace directories"""
  jsondata = {"path": directory_path}
  response = execute_rest_api_call(post_request, get_api_config(dbricks_instance, api_topic = "workspace", api_call_type = "mkdirs"), dbricks_pat, jsondata)
  return response
