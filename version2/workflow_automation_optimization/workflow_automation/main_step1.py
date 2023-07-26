# Databricks notebook source
# DBTITLE 1,Create Notebook Widgets
# notebook source
dbutils.widgets.removeAll()
# dbutils.widgets.remove("source")
dbutils.widgets.dropdown("source", "git", ["git", "workspace"])

# shared single node cluster id
# dbutils.widgets.remove("shared_single_node_cluster")
dbutils.widgets.dropdown("shared_single_node_cluster", "0329-145545-rugby794", ["0329-145545-rugby794"])

# shared workflow clutser id
# dbutils.widgets.remove("shared_workflow_cluster")
dbutils.widgets.dropdown("shared_workflow_cluster", "0329-145545-rugby794", ["0329-145545-rugby794"])

# COMMAND ----------

# DBTITLE 1,Get Workflow Parameters
# notebook_source = dbutils.widgets.get('source') # source
# shared_single_node_cluster = dbutils.widgets.get('shared_single_node_cluster') # shared_single_node_cluster
# shared_workflow_cluster = dbutils.widgets.get('shared_workflow_cluster') # shared_workflow_cluster
# duplicate_workflows_allowed = dbutils.widgets.get('duplicate_workflows_allowed') # workflow action
# git_branch = dbutils.widgets.get('git_branch') # git branch

# COMMAND ----------

# DBTITLE 1,Get Widget Values
notebook_source = dbutils.widgets.get('source') # source
shared_single_node_cluster = dbutils.widgets.get('shared_single_node_cluster') # shared_single_node_cluster
shared_workflow_cluster = dbutils.widgets.get('shared_workflow_cluster') # shared_workflow_cluster

# COMMAND ----------

# DBTITLE 1,Library Imports, General Functions, and Requests Base Functions
# MAGIC %run "./dbricks_api/general/main"

# COMMAND ----------

# DBTITLE 1,Databricks API 2.0 Functions (Workspace, Jobs, Perms, etc)
# MAGIC %run "./dbricks_api/api_calls/main"

# COMMAND ----------

# DBTITLE 1,Set Local Variables
# base directory of workflow automation files
nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get() # full notebook path
# event_wait_nb = f"{nb_path.rsplit('/', 1)[0]}/dmi-databricks/DEI_DATA_LS/Create_Files_On_Completion" # event wait notebook path create parameter file
# this cluster is used to create the parameter files in /dbfs/mnt/mshedw/silver/file_watcher
# event_wait_cluster_id = "0223-201818-urkj98oc" # don't change
base_directory = f"/Workspace{nb_path.rsplit('/', 1)[0]}/workflow_automation_files"
print(f"base_directory: {base_directory}")

# file watcher scripts list
file_watcher_scripts_list = ['Delete_Files_On_Completion', 'Create_Files_On_Completion', 'Wait_On_Dependent_Workflow']

# create workflow variables if notebook source is git (optional)
git_provider = "github"
git_branch = "feature/wf_Refresh_MCMD"
git_url = "https://github.com/robert-altmiller/workflow_automation_optimization"

# create workflow status variable
duplicate_workflows_allowed = "False" # eliminate duplicate workflows
on_success_email = "robert.altmiller@databricks.com"
on_failure_email = "robert.altmiller@databricks.com"
pause_status = "PAUSED"

# notebook creation variable
language = "PYTHON"

# workflow permission variables
group_name = "ONTADA_DMI_DATABRICKS_CONS"
permissions_level = "CAN_MANAGE"


# notebook base path
notebook_base_path = f"/Workspace{nb_path.rsplit('/', 1)[0]}/dmi-databricks" #"/Workspace/Repos/Repo/dmi-databricks/"
try: 
    os.makedirs(notebook_base_path)
    print(f"{notebook_base_path} created successfully.....")
except: print(f"{notebook_base_path} already exists.....")
notebook_base_path = notebook_base_path.split("/Workspace")[1] # writes into the repos folder


# create json file export, payload, and params path
workflow_json_file_export_path = f"/Workspace{nb_path.rsplit('/', 1)[0]}/workflow_json_files" # "/Workspace/Repos/Repo/dmi-databricks/DEI_DATA_LS/workflow_json_files"
try: os.makedirs(f"{workflow_json_file_export_path}/export")
except: print('')
try: os.makedirs(f"{workflow_json_file_export_path}/payload")
except: print('')
try: os.makedirs(f"{workflow_json_file_export_path}/params")
except: print('')
try: os.remove(f"{workflow_json_file_export_path}/params/parameters.txt")
except: print('')

# COMMAND ----------

# DBTITLE 1,Run Workflow Automation (Create Notebooks, Workflows, Jobs, Permissions)
# get a list of the files in the base_directory
files = os.listdir(base_directory)

for file in files:

    # workflow name
    workflowname = os.path.basename(file).split(".")[0]
    print(f"Workflow name: {workflowname}\n")

    # create workflow metadata pandas dataframe
    df = pd.read_excel(
        os.path.join(base_directory, file),
        sheet_name="API",
    )

    # rename dataframe columns
    df = df.rename(
        columns =
        {
            "Procedure Name": "task_key",
            "Dependant Procedure/Task": "depends_on",
            "Scripts Location": "notebook_path",
            "Base Parameters": "base_parameters",
            "Schedule": "Schedule",
        }
    )
    
    # print notebook source
    print(f"The source is: {notebook_source}\n")
    
    # sanity check to ensure notebook source is 'git' or 'workspace'
    if notebook_source.lower() == 'git': source = "GIT"        
    elif notebook_source.lower() == 'workspace':
        source = "WORKSPACE"
        # add a new column 'notebook_path' to the workflow metadata dataframe
        df["notebook_path"] = notebook_base_path + "/" + df["notebook_path"]
    else:
        print("Source passed not correct, it should be either workspace or github\n")
        sys.exit(1) # exit


    # pre-defined chron job schedule
    schedule = df["Schedule"].iloc[0]


    tasks = []
    # iterate over each row of the dataframe to build out workflow
    for index, row in df.iterrows():
        
        notebook_path = row["notebook_path"]
        print(f"Notebook Path: {notebook_path}\n")

        # check if its file watcher script then assign single node or shared workflow cluster
        if notebook_path.split("/")[-1] in file_watcher_scripts_list: 
            cluster_id = shared_single_node_cluster
        else: cluster_id = shared_workflow_cluster        
        

        if notebook_source == 'workspace':
            
            # api call to create workspace directory if not exist
            directory_path = os.path.split(notebook_path)[0]
            response = create_workspace_directory(databricks_instance, databricks_pat, directory_path)
            print(f"Workspace directory '{directory_path}' was created successfully: {response}\n")

            # api call to create workspace notebook if not exist
            response = create_workspace_notebook(databricks_instance, databricks_pat, notebook_path, language)
            print(f"Notebook '{notebook_path}' created successfully: {response}\n")


        task = {
            "task_key": row["task_key"],
            "notebook_task": {
                "notebook_path": row["notebook_path"],
                "source": source,
            },
            "depends_on": [{"task_key": task} for task in row["depends_on"].split(";")]
            if not pd.isna(row["depends_on"])
            else [],            
            "existing_cluster_id": cluster_id,
        }

        # check if there are any base parameters
        base_parameters = row["base_parameters"]
        print(f"Base parameters: {base_parameters}\n")
        base_parameters_dict = {}
        
        if pd.isna(base_parameters): pass # no base parameters
        else: # base parameters exist
            base_param_list = base_parameters.split(";")
            print(f"Base parameter list: {base_param_list}\n")
            i = 0
            for param in base_param_list:

                # create informatica event wait parameter file in dbfs for databricks workflow using shared single node cluster
                if "wait_on_dependent_workflow" in notebook_path.lower() and param.lower().endswith(".txt"):
                    result_filepath = write_new_file(f"{workflow_json_file_export_path}/params", f"parameters.txt", f"{param};", "a")
                    print(f"{result_filepath}\n")

                if i == 0: dict_key = "file_name"
                else: dict_key = "file_name" + str(i)
                base_parameters_dict[dict_key] = param
                i = i + 1
            task["notebook_task"]["base_parameters"] = base_parameters_dict

        # append the task and end of loop
        tasks.append(task)

    
    # api call to delete workflow if duplicate_workflows_allowed = False
    if duplicate_workflows_allowed.lower() == "false":
        job_id = get_workflow_jobid(databricks_instance, databricks_pat, workflowname)
        response = delete_workflow(databricks_instance, databricks_pat, job_id)
        print(f"Workflow '{workflowname}' deleted successfully: {response}\n") # workflow deletion response


    # api call to create workflow (take note of all the parameters for json payload)
    response = create_workflow(databricks_instance, databricks_pat, workflowname, tasks, on_success_email,
                               on_failure_email, schedule, pause_status, notebook_source, git_url, git_provider, git_branch)
    print(f"Workflow '{workflowname}' created successfully: {response}\n") # workflow creation response

    # add / patch group permission can_manage_run to the workflow
    if response.status_code == 200:
        result_filepath = write_new_file(f"{workflow_json_file_export_path}/payload", f"{workflowname}.json", response.text, "a")
        print(f"{result_filepath}\n")


        # get the workflow job id
        job_id = response.json()["job_id"]
    

        # api call to add workflow permissions (ep = endpoint)
        response = add_workflow_permissions(databricks_instance, databricks_pat, job_id, group_name, permissions_level)
        print(f"Updated workflow job permissions for job id {job_id}: {response}\n")
        

        # get workflow job details and create json file export of each workflow (ep = endpoint)
        response = get_workflow_job_details(databricks_instance, databricks_pat, job_id)
        print(f"Job file export created for job id {job_id}: {response}\n")

        if response.status_code == 200: # then create json file export for the workflow
            result_filepath = write_new_file(f"{workflow_json_file_export_path}/export", f"{workflowname}.json", response.text, "a")
            print(f"{result_filepath}\n")
        
        else: # error created json export file
            print(response.content)
            print("Error creating json export file\n")


    else: # could not trigger pipeline
        print(response.content)
        print("Error triggering the pipeline\n")

# COMMAND ----------


