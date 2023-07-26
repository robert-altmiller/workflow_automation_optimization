# Databricks notebook source
# DBTITLE 1,Databricks API 2.0 - Add Workflow Permissions
def add_workflow_permissions(dbricks_instance = None, dbricks_pat = None, job_id = None, group_name = None, permissions_level = None):
  """add workflow permissions (ep = endpoint)"""
  jsondata = {
    "access_control_list": [
        {
            "group_name": group_name,
            "permission_level": permissions_level
        }
    ]
  }
  jobperms_config = get_api_config(dbricks_instance, api_topic = "permissions", api_call_type = "jobs")
  jobperms_ep = jobperms_config["api_full_url"]
  jobperms_config["api_full_url"] = f"{jobperms_ep}/{str(job_id)}" # add job iud
  response = execute_rest_api_call(patch_request, jobperms_config, dbricks_pat, jsondata)
  return response
