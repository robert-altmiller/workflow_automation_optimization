# Databricks notebook source
parameters = dbutils.notebook.entry_point.getCurrentBindings()
print(parameters)

# COMMAND ----------

import sys, os

if bool(parameters):
    for param in parameters:
        file_name = dbutils.widgets.get(param)
        file_path = "/mnt/mshedw/silver/file_watcher/" + file_name
        print(file_path)
        try:
            dbutils.fs.ls(file_path)
            dbutils.fs.rm(file_path)
        except Exception as e :
            if("java.io.FileNotFoundException" in str(e)):
                print("File doesn't exists")
#               sys.exit(1)

else:
    print("No Parameters passed")
    sys.exit(1)
