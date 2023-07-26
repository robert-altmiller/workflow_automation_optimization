# Databricks notebook source
# DBTITLE 1,Import Libraries
import requests, json

# COMMAND ----------

# DBTITLE 1,Set Local Variable Parameters
databricks_instance = str(spark.conf.get("spark.databricks.workspaceUrl"))
databricks_token = str(dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get())
secret_scope_name = "tblu_secret_scope"


# COMMAND ----------

# DBTITLE 1,Create or Delete Secret Scope
def create_delete_secret_scope(instance, token, scope_name, action = "create"):
    """create or delete a secret scope; action is 'create' or 'delete'"""
    if action == "create": # create secret scope
        api_url = f"https://{instance}/api/2.0/secrets/scopes/create"
    else: # delete secret scope 
        api_url = f"https://{instance}/api/2.0/secrets/scopes/delete"

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"scope": scope_name}

    try:
        response = requests.post(api_url, headers = headers, data = json.dumps(payload))
        response.raise_for_status()
        print(f"Secret scope '{scope_name}' {action} successfully.")
    except requests.exceptions.RequestException as e:
        print(f"Failed to {action} secret scope '{scope_name}': {str(e)}")

create_delete_secret_scope(databricks_instance, databricks_token, secret_scope_name, "delete")
create_delete_secret_scope(databricks_instance, databricks_token, secret_scope_name, "create")

# COMMAND ----------

# DBTITLE 1,Add Secrets to the Secret Scope
def add_delete_secret_to_scope(instance, token, scope_name, secret_name, secret_value = None, action = "create"):
    """create or delete a secret scope secret; action is 'create' or 'delete'"""
    if action == "create": # create secret scope secret
        api_url = f"https://{instance}/api/2.0/secrets/put"
        payload = {"scope": scope_name, "key": secret_name, "string_value": secret_value}
    else: # delete secret scope secret
        api_url = f"https://{instance}/api/2.0/secrets/delete"
        payload = {"scope": scope_name, "key": secret_name}
    
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    try:
        response = requests.post(api_url, headers = headers, data = json.dumps(payload))
        response.raise_for_status()
        print(f"Secret '{secret_name}' {action} to scope '{scope_name}' successfully.")
    except requests.exceptions.RequestException as e:
        print(f"Failed to {action} secret '{secret_name}' to scope '{scope_name}': {str(e)}")

add_delete_secret_to_scope(databricks_instance, databricks_token, secret_scope_name, "TBLU_LOGIN", action = "delete")
add_delete_secret_to_scope(databricks_instance, databricks_token, secret_scope_name, "TBLU_PASSWORD", action = "delete")
add_delete_secret_to_scope(databricks_instance, databricks_token, secret_scope_name, "TBLU_LOGIN", "dzwo2vp", action = "create")
add_delete_secret_to_scope(databricks_instance, databricks_token, secret_scope_name, "TBLU_PASSWORD", "", action = "create")

# COMMAND ----------

# DBTITLE 1,Add Permissions To Secret Scope
def add_delete_permissions_to_scope(instance, token, scope_name, principal, permission = "MANAGE", action = "create"):
    """
    add or remove permissions to databricks secret scope; action is 'create' or 'delete'
    permissions = MANAGE or WRITE or READ
    """
    if action == "create": # create acls
        api_url = f"https://{instance}/api/2.0/secrets/acls/put"
        payload = {"scope": scope_name, "principal": principal, "permission": permission}
    else:  # delete acls
        api_url = f"https://{instance}/api/2.0/secrets/acls/delete"
        payload = {"scope": scope_name, "principal": principal}

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    try:
        response = requests.post(api_url, headers = headers, data = json.dumps(payload))
        response.raise_for_status()
        print(f"Permission '{permission}' for principal {principal} {action} to scope '{scope_name}' successfully.")
    except requests.exceptions.RequestException as e:
        print(f"Failed to {action} permission '{permission}' for principal {principal} to scope '{scope_name}': {str(e)}")

# add_delete_permissions_to_scope(databricks_instance, databricks_token, secret_scope_name, "robert.altmiller@mckesson.com", "MANAGE", action = "delete")
# add_delete_permissions_to_scope(databricks_instance, databricks_token, secret_scope_name, "sibi.augustin@mckessonc.com", "MANAGE", action = "delete")
add_delete_permissions_to_scope(databricks_instance, databricks_token, secret_scope_name, "zahar.hilkevich@mckesson.com", "MANAGE", action = "create")
# add_delete_permissions_to_scope(databricks_instance, databricks_token, secret_scope_name, "sibi.augustin@mckessonc.com", "MANAGE", action = "create")

# COMMAND ----------

# DBTITLE 1,Read Secrets From the Secret Scope for Use in Shell Script
tblu_login = dbutils.secrets.get(secret_scope_name, "TBLU_LOGIN")
tblu_password = dbutils.secrets.get(secret_scope_name, "TBLU_PASSWORD")
