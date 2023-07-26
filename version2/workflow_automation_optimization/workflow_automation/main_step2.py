# Databricks notebook source
# DBTITLE 1,Library Imports
import os

# COMMAND ----------

# DBTITLE 1,Import Notebooks
# MAGIC %run "./dmi-databricks/DEI_DATA_LS/Create_Files_On_Completion"

# COMMAND ----------

# DBTITLE 1,Set Local Variables
# parameter file path
params_filepath = "./workflow_json_files/params/parameters.txt"
params_path_exists = False
if os.path.exists(params_filepath):
    params_path_exists = True
print(f"params_filepath '{params_filepath}' exists: {params_path_exists}")

# COMMAND ----------

# DBTITLE 1,Create Parameter Filename From DBFS
if params_path_exists == True:
    try:
        f = open(params_filepath, 'r')
        parameter_filename = f.read()
        f.close()
        if ";" in parameter_filename: # we have more than one file
            parameter_filename = parameter_filename.split(";")
        print(f"parameter_filename: {parameter_filename}")
    except: print(f"unable to open '{params_file_path}'")

# COMMAND ----------

# DBTITLE 1,Create Parameter File(s) in DBFS
if params_path_exists == True:
    if type(parameter_filename) == list:
        for filename in parameter_filename:
            if len(filename) > 0:
                create_parameter_file([filename])
    else: 
        create_parameter_file([parameter_filename])
