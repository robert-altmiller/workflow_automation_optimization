# Databricks notebook source
import time
import sys

init_time = 0
base_location = "dbfs:/mnt/mshedw/silver/file_watcher/"
file_watcher = dbutils.widgets.get('file_name')
threshold = int(dbutils.widgets.get('file_name1'))

while init_time < threshold:
    try:
        path = base_location + file_watcher
        print("Searching for", path)
        dbutils.fs.ls(path)
        break
    except Exception as e :
        if("java.io.FileNotFoundException" in str(e)):
            print("Sleep Time")
            time.sleep(600)
            init_time+=600
        else:
            print("Other exception", e)

if (init_time >= threshold):
    sys.exit(1)
