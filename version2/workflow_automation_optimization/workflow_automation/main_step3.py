# Databricks notebook source
# DBTITLE 1,Library Imports
import os, shutil

# COMMAND ----------

# DBTITLE 1,Set Local Variables
# parameter file path
params_filepath = "./workflow_json_files/params/parameters.txt"
wf_auto_files_fldr_path = "./workflow_automation_files/"
wf_auto_files_valid_fldr_path = "./workflow_automation_files_validated/"

# COMMAND ----------

# DBTITLE 1,Remove Parameters Text File
# try:
#     os.remove(params_filepath)
#     print(f"removed file: {params_filepath}")
# except: print(f"unable to remove {params_filepath}")

# COMMAND ----------

# DBTITLE 1,Move Automation Excel Files into Workflow Validation Folder
# fetch all files
for file_name in os.listdir(wf_auto_files_fldr_path):
    # construct full file path
    source = wf_auto_files_fldr_path + file_name
    destination = wf_auto_files_valid_fldr_path + file_name
    # move only files
    if os.path.isfile(source):
        shutil.move(source, destination)
        print(f"Moved: {source} into {destination}")
