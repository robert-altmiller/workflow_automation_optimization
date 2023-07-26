# Databricks notebook source
# DBTITLE 1,Databricks API 2.0 - Create Workflow
def create_workflow(dbricks_instance = None, dbricks_pat = None, wfname = None, wftasks = None, wf_onsuccess_email = None, 
                     wf_on_failure_email = None, schedule = None, pause_status = None, nb_source = None, giturl = None, gitprovider = None, gitbranch = None):
  """create workflow"""
  # get workflow creation payload
  # wf = workflow, nb = notebook
  jsondata = {
      "name": wfname,
      "tasks": wftasks,
      "email_notifications": {
          "on_start": [],
          "on_success": [wf_onsuccess_email],
          "on_failure": [wf_on_failure_email]
      },
      "schedule": {
          "quartz_cron_expression": schedule,
          "timezone_id": "UTC",
          "pause_status": pause_status,
      }
  }
  # additional workflow payload data
  if nb_source == 'git':
      jsondata["git_source"] = {
          "git_url": giturl,
          "git_provider": gitprovider,
          "git_branch": gitbranch
      }
  response = execute_rest_api_call(post_request, get_api_config(dbricks_instance, api_topic = "jobs", api_call_type = "create"), dbricks_pat, jsondata)
  return response

# COMMAND ----------

# DBTITLE 1,Databricks API 2.0 - Delete Workflow
def delete_workflow(dbricks_instance = None, dbricks_pat = None, job_id = None):
  """delete workflow"""
  jsondata = {"job_id": job_id}
  response = execute_rest_api_call(post_request, get_api_config(dbricks_instance, api_topic = "jobs", api_call_type = "delete"), dbricks_pat, jsondata)
  return response

# COMMAND ----------

# DBTITLE 1,Databricks API 2.0 - Get Workflow Job Details
def get_workflow_job_details(dbricks_instance = None, dbricks_pat = None, job_id = None):
  """get workflow job id (ep = endpoint)"""
  jsondata = None
  job_config = get_api_config(dbricks_instance, api_topic = "jobs", api_call_type = "get")
  jobid_ep = job_config["api_full_url"]
  job_config["api_full_url"] = f"{jobid_ep}?job_id={str(job_id)}"
  response = execute_rest_api_call(get_request, job_config, dbricks_pat, jsondata)
  return response

# COMMAND ----------

# DBTITLE 1,Databricks API 2.0 - List Workflows
def list_workflows(dbricks_instance = None, dbricks_pat = None):
  """list all workflows"""
  jsondata = None
  response = execute_rest_api_call(get_request, get_api_config(dbricks_instance, api_topic = "jobs", api_call_type = "list"), dbricks_pat, jsondata)
  return response

# COMMAND ----------

# DBTITLE 1,Databricks API 2.0 - Get Workflow Job Id
def get_workflow_jobid(dbricks_instance = None, dbricks_pat = None, workflowname = None):
  """get workflow job id (wf = workflow)"""
  response = list_workflows(dbricks_instance, dbricks_pat)
  if response.status_code == 200: # api call success
    for job in response.json()["jobs"]:
        wf_name = job["settings"]["name"]
        job_id = job["job_id"]
        if workflowname == wf_name: # we matched workflow name
          return job_id # return matched workflow job id
  return None
    
