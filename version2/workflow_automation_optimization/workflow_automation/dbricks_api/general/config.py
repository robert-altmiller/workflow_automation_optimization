# Databricks notebook source
# DBTITLE 1,Import Libraries
# MAGIC %run "./libraries"

# COMMAND ----------

# DBTITLE 1,Import Generic Functions
# MAGIC %run "./general_functions"

# COMMAND ----------

# DBTITLE 1,Configuration Class Initialization
class Config:
# configuration class definition

    # class constructor    
    def __init__(self):

        # variables
        self.DATABRICKS_USER_NAME = str(spark.sql('select current_user() as user').collect()[0]['user'])
        self.DATABRICKS_INSTANCE = str(spark.conf.get("spark.databricks.workspaceUrl"))
        self.DATABRICKS_PAT = str(dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get())
        self.format_config_vars()
        

    def format_config_vars(self):
        """
        additional formatting for configuration variables
        this function is optional if you need it
        """
        return None


    def get_config_vars(self):
        # get class configuration variables
        config = Config()
        return vars(config)


    def print_config_vars(self):
        # get configuration variables in a python dictionary
        variables = self.get_config_vars()
        print("configuration variables:")
        vars_list = []
        for key, val in variables.items():
            print(f"{key}: {val}")
        print("\n")

# COMMAND ----------

# DBTITLE 1,Variables Initialization

# configuration class object
config = Config()
# print configuration variables
config.print_config_vars()


# get configuration variables
config = config.get_config_vars()


# databricks workspace instance
databricks_instance = config["DATABRICKS_INSTANCE"]
print(f"databricks_instance: {databricks_instance}")

# databricks personal access token
databricks_pat = config["DATABRICKS_PAT"]
print(f"databricks_pat: {databricks_pat}")

# databricks user name
databricks_username = config["DATABRICKS_USER_NAME"]
print(f"databricks_username: {databricks_username}")

