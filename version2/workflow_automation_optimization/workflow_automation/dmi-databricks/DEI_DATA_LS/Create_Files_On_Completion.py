# Databricks notebook source
# DBTITLE 1,Create Parameter File for Informatics Event Wait Activities
def create_parameter_file(parameters = None):
    """
    create parameter file name for event wait activities in Informatica workflows
    parameters should be a python list
    """
    for param in parameters:
        file_name = param
        folder_path = f"/dbfs/mnt/mshedw/silver/file_watcher"
        file_path = f"{folder_path}/{file_name}"
        if os.path.exists(file_path):
            print("File already exists " + file_path)
            return "PATH ALREADY EXISTS"
        else:
            try:
                f = open(file_path, "a") # params: a = append, w = write
                f.write(' ')  
                f.close()
                print(f"SUCCESS: file path {file_path} created successfully....")
                return "SUCCESS"
            except: 
                print(f"FAIL: file path {file_path} not created........")
                return "FAIL"
