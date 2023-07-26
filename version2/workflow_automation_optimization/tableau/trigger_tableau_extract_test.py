# Databricks notebook source
# DBTITLE 1,PIP Install Requirements
# MAGIC %pip install xmltodict

# COMMAND ----------

# DBTITLE 1,Import Libraries
import os, sys, importlib
sys.path.append(os.environ['PROVIDER_REPO_PATH']) if os.environ['PROVIDER_REPO_PATH'] not in sys.path else sys.path
importlib.import_module('scripts.load_notebook_config').load_notebook_config(spark, dbutils, os.environ['PROVIDER_REPO_PATH'] + '/scripts/notebook_config_py.json')

from tblu_common_functions import *

# COMMAND ----------

# DBTITLE 1,Refresh Tableau Workbooks and Datasources
# secret scope has the login and password to use Tableau API
secret_scope_name = "tblu_secret_scope"

refresh_tableau_wb_workflow(
    tblu_username = dbutils.secrets.get(secret_scope_name, "TBLU_LOGIN"), 
    tblu_password = dbutils.secrets.get(secret_scope_name, "TBLU_PASSWORD"),
    tblu_project = "Practice Insights Super User-Tst",
    tblu_refresh_obj = "Activity Dashboard-Tst",
    tblu_refresh_method = "workbook"
)

refresh_tableau_wb_workflow(
    tblu_username = dbutils.secrets.get(secret_scope_name, "TBLU_LOGIN"), 
    tblu_password = dbutils.secrets.get(secret_scope_name, "TBLU_PASSWORD"),
    tblu_project = "Practice Insights-Tst",
    tblu_refresh_obj = "MCMW-Tst",
    tblu_refresh_method = "datasource"
)
