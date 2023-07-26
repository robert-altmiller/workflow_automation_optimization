# Databricks notebook source
# DBTITLE 1,PIP Install Requirements
# MAGIC %pip install xmltodict

# COMMAND ----------

# DBTITLE 1,Import Libraries
import os, sys, importlib, xmltodict
sys.path.append(os.environ['PROVIDER_REPO_PATH']) if os.environ['PROVIDER_REPO_PATH'] not in sys.path else sys.path
importlib.import_module('scripts.load_notebook_config').load_notebook_config(spark, dbutils, os.environ['PROVIDER_REPO_PATH'] + '/scripts/notebook_config_py.json')

from tblu_common_functions import *

# COMMAND ----------

# DBTITLE 1,Tableau Table Refresh Variables
#workbook, "Activity Dashboard-Tst" , project, "Practice Insights Super User-Tst"

# workbooks_projects_list = ["Practice Insights Super User-Tst"]
# workbooks_list = ["Activity Dashboard-Tst"]

workbooks_projects_list = ["Practice Insights"]
workbooks_list = ["Patient Explorer"]

# datasources_projects_list = ["Practice Insights"]
# datasources_list = ["MCMW"]

# COMMAND ----------

# DBTITLE 1,Get Tableau Authentication Token and SiteId
xml_response_status, xml_response = get_tableau_auth_token_data("dzwo2vp", "Qprjt37$bHmBUV@c")
json_response = convert_xml_to_json(xmltodict, xml_response)#
# print(json_response)
json_respons_obj = json.loads(json_response)
tblu_token = json_respons_obj["tsResponse"]["credentials"]["@token"]
tblu_siteid = json_respons_obj["tsResponse"]["credentials"]["site"]["@id"]
print(f"tblu_token: {tblu_token}")
print(f"tblu_siteid: {tblu_siteid}")

# COMMAND ----------

# DBTITLE 1,Get Tableau SiteId Workbooks
xml_response_status, xml_response = get_tableau_siteid_workbooks(tblu_token, tblu_siteid)
json_response = convert_xml_to_json(xmltodict, xml_response)
# print(json_response)
json_respons_obj = json.loads(json_response)
workbooks_json = json_respons_obj["tsResponse"]["workbooks"]["workbook"]
workbooks_data = {}
for workbook in workbooks_json:
    proj_name = workbook["project"]["@name"]
    wb_name = workbook["@name"]
    if proj_name in workbooks_projects_list:
        if wb_name in workbooks_list:
            print(f"found project '{proj_name}' and workbook name '{wb_name}' and workbook id '{workbook['@id']}'")
            workbooks_data[f"{proj_name}-{wb_name}"] = workbook["@id"]
print(workbooks_data)

# COMMAND ----------

# DBTITLE 1,Get Tableau SiteId Datasources
xml_response_status, xml_response = get_all_tableau_siteid_datasources(tblu_token, tblu_siteid)
json_response = convert_xml_to_json(xmltodict, xml_response)
#print(json_response)
json_respons_obj = json.loads(json_response)
datasources_json = json_respons_obj["tsResponse"]["datasources"]["datasource"]
datasources_data = {}
for datasource in datasources_json:
    proj_name = datasource["project"]["@name"]
    datasource_name = datasource["@name"]
    if proj_name in datasources_projects_list:
        if datasource_name in datasources_list:
            print(f"found project '{proj_name}' and datasource name '{datasource_name}' and datasource id '{datasource['@id']}'")
            datasources_data[f"{proj_name}-{datasource_name}"] = datasource["@id"]
print(datasources_data)

# COMMAND ----------

# DBTITLE 1,Refresh All Workbooks with Tableau WorkbookIds
jobids_list = []
for i in range(len(workbooks_projects_list)):
    # workbook unique key identifier (proj-workbookname)
    wb_pk_id = f"{workbooks_projects_list[i]}-{workbooks_list[i]}"
    
    print(f"workbook primary key id: {wb_pk_id}")
    print(f"starting workbook refresh project '{workbooks_projects_list[i]}' and workbook name '{workbooks_list[i]}' and workbookid '{workbooks_data[wb_pk_id]}'")
    
    xml_response_status, xml_response = get_tableau_refresh(tblu_token, tblu_siteid, workbooks_data[wb_pk_id], "workbook")
    json_response = convert_xml_to_json(xmltodict, xml_response)
    json_respons_obj = json.loads(json_response)
    print(json_response)
    try: # get jobid for tableau workbook refresh
        tblu_jobid = json_respons_obj["tsResponse"]["job"]["@id"]
    except: # workbook refresh job has already been queued
        tblu_jobid = "NO_JOB_ID"
        tblu_exception = json_respons_obj["tsResponse"]["error"]
        print(f"ERROR: {tblu_exception}")
    jobids_list.append(tblu_jobid)

# COMMAND ----------

# DBTITLE 1,Get Tableau Workbook Refresh Job Status
for tblu_jobid in jobids_list:
    xml_response_status, xml_response = get_tableau_job_status(tblu_token, tblu_siteid, tblu_jobid)
    json_response = convert_xml_to_json(xmltodict, xml_response)
    print(json_response)
    json_respons_obj = json.loads(json_response)
    try:
        tblu_job_progress = int(json_respons_obj["tsResponse"]["job"]["@progress"])
        print(f"tableau job status progress for jobid {tblu_jobid}: in-progress....")
    except: 
        tblu_exception = json_respons_obj["tsResponse"]["error"]
        print(f"ERROR: {tblu_exception}")

# COMMAND ----------

# DBTITLE 1,Refresh All Datasources with Tableau DatasourceIds
jobids_list = []
for i in range(len(datasources_projects_list)):
    # workbook unique key identifier (proj-workbookname)
    ds_pk_id = f"{datasources_projects_list[i]}-{datasources_list[i]}"
    
    print(f"datasource primary key id: {ds_pk_id}")
    print(f"starting datasource refresh project '{datasources_projects_list[i]}' and datasource name '{datasources_list[i]}' and datasourceid '{datasources_data[ds_pk_id]}'")
    
    xml_response_status, xml_response = get_tableau_refresh(tblu_token, tblu_siteid, datasources_data[ds_pk_id], "datasource")
    json_response = convert_xml_to_json(xmltodict, xml_response)
    json_respons_obj = json.loads(json_response)
    print(json_response)
    try: # get jobid for tableau datasource refresh
        tblu_jobid = json_respons_obj["tsResponse"]["job"]["@id"]
    except: # datasource refresh job has already been queued
        tblu_jobid = "NO_JOB_ID"
        tblu_exception = json_respons_obj["tsResponse"]["error"]
        print(f"ERROR: {tblu_exception}")
    jobids_list.append(tblu_jobid)

# COMMAND ----------

# DBTITLE 1,Get Tableau Datasource Refresh Job Status
for tblu_jobid in jobids_list:
    xml_response_status, xml_response = get_tableau_job_status(tblu_token, tblu_siteid, tblu_jobid)
    json_response = convert_xml_to_json(xmltodict, xml_response)
    print(json_response)
    json_respons_obj = json.loads(json_response)
    try:
        tblu_job_progress = int(json_respons_obj["tsResponse"]["job"]["@progress"])
        print(f"tableau job status progress for jobid {tblu_jobid}: in-progress....")
    except: 
        tblu_exception = json_respons_obj["tsResponse"]["error"]
        print(f"ERROR: {tblu_exception}")

# COMMAND ----------

# DBTITLE 1,Sign Out Of Tableau API
xml_response_status, xml_response = get_tableau_api_signout(tblu_token)
print(f"signed out of tableau response status {xml_response_status}....")

# COMMAND ----------

# DBTITLE 1,Get All Tableau Server JobIds
xml_response_status, xml_response = get_all_tableau_server_jobids(tblu_token, tblu_siteid)
json_response = convert_xml_to_json(xmltodict, xml_response)
print(json_response)
json_respons_obj = json.loads(json_response)
