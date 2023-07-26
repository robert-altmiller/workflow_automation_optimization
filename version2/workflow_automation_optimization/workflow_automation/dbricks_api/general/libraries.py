# Databricks notebook source
# DBTITLE 1,Get Requirements File Path
import os

# get current notebook path and create a path to requirements.txt for library installs
nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
split_str = "workflow_automation"
requirementspath = f"{nb_path.rsplit(split_str, 1)[0]}{split_str}/dbricks_api/general/requirements.txt"


# need to figure out if we are running this from /Workspace/Users or /Repos
if "Users" in nb_path or "Repos" in nb_path:
  requirementspath = f"/Workspace{requirementspath}"

# set environment variables
os.environ["requirementspath"] = requirementspath
print(requirementspath)

# COMMAND ----------

# DBTITLE 1,Install Library Requirements
# MAGIC %sh
# MAGIC
# MAGIC # local train data filepath
# MAGIC for path in $requirementspath 
# MAGIC   do 
# MAGIC     requirementspath="$path" 
# MAGIC   done
# MAGIC echo $requirementspath
# MAGIC
# MAGIC pip install -r $requirementspath

# COMMAND ----------

# DBTITLE 1,Library Imports
# library and file imports
import json, time, requests, hashlib, string, random, pathlib, re, shutil, urllib.parse
import openpyxl
from datetime import datetime

# numpy and pandas
import pandas as pd
import numpy as np

# pyspark imports
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.dbutils import DBUtils
