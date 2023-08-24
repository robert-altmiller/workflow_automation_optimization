# Automated Databricks Workflow Creation Tool

Automating the creation of 'simple' and 'complex' Databricks workflows can help with Databrick migrations from Informatica, Oracle, and SQL Server.  Creating and wiring workflows together that execute notebooks to orchestrate the creation, deletion, and updates of data across Bronze, Silver and Gold can be complex and time consuming.  This tool was created originally to help with Informatica Workflow migrations specifically but it has a much wider use with generic Databricks workflow creation.

### Link to Github Repo: https://github.com/robert-altmiller/workflow_automation_optimization

## Features of the Databricks Workflow Creation Tool

The Databricks Workflow Creation Tool has the following features:

- Create workflows that can run Notebooks in Github or in the local Databricks workspace.
- If running workflow from Workspace code will create Notebooks and Directory structures based on the Scripts location path in the Excel template.
- Create and add custom workflow parameters.
- Apply group permissions to workflows.
- Boolean flags enable / disable the creation of duplicate workflows.
- Code exports the workflow configuration in JSON format.
- Workflow Schedule is set in Excel template.
- Workflow creation input parameters are injected using Excel template.
- Email alerts are automatically set on workflows if they fail.
- Workflow cluster is auto assigned using Databricks widgets.

### ***IMPORTANT***: You can run this workflow with the existing Excel workflow template in the Github Repository.  If you want to see the execution before created a 'modified' Excel workflow template you can skip the section below on 'How to Update Workflow Creation Excel Template and go directory to 'How to Use the Databricks Workflow Creation Tool'.

## How to Update Workflow Creation Excel Template

- After cloning down or forking the repository the starting Excel workflow template can be found in the following location:

![excel_template_loc.jpg](/readme_images/excel_template_loc.jpg)

- The Excel workflow template below needs to be modified for the creation of any new Databricks workflow.
- ***IMPORTANT***: Please name the Excel sheet 'API'

![excel_template_mod1.jpg](/readme_images/excel_template_mod1.jpg)

- After making changes to the Excel workflow template please upload it to the following directory:

![excel_template_loc.jpg](/readme_images/excel_template_loc.jpg)

## How to Use the Databricks Workflow Creation Tool

- The code file which creates the Databricks workflow can be found in the following location:

![main_prog.jpg](/readme_images/main_prog.jpg)

- Update widgets cluster ids for 'shared_workflow_cluster' and 'shared single node' cluster.

![main_prog_run1.jpg](/readme_images/main_prog_run1.jpg)

- The parameters to update in the 'main_step1' notebook are the following:

![main_prog_run2.jpg](/readme_images/main_prog_run2.jpg)

- Change 'git_provider' to 'github' or 'workspace' depending where code needs to be run from.  If value is 'workspace' then variables 'git_branch' and 'git_url' are ignored.
- Change 'git_branch' and 'git_url' to the appropriate values if using Github for Notebook execution.

- Change 'duplicate_workflows_allowed' to 'True' or 'False' to enable or disable duplicate workflow creation.
- Change 'on_success_email' and 'on_failure_email' to the appropriate person or group email address for workflow notifications.
- Change 'pause_status' to 'ACTIVE' or 'PAUSED' to set workflow status after workflow creation is completed.

- Change 'group_name' to the name of the Databricks group who will be able to run and manage the workflow.
- Change 'permissions_level' to the level of workflow access (e.g. IS_OWNER, CAN_VIEW, CAN_MANAGE, CAN_MANAGE_RUN) for the 'group_name'.

![group_perms.jpg](/readme_images/group_perms.jpg)

- After updating the workflow run parameters like mentioned above you can run the 'main_step1' notebook entirely and then check for the created workflow.

![created_workflow1.jpg](/readme_images/created_workflow1.jpg)

![created_workflow2.jpg](/readme_images/created_workflow2.jpg)

## Additional Workflow Creation Program Functionality

- There are two more additional notebooks for 'Main' program functionality:

![added_main_functionality.jpg.jpg](/readme_images/added_main_functionality.jpg)

- The 'main_step2' notebook is simply used for copying 'parameter.txt' file(s) to a specific DBFS location.  This was only relevant for Informatica workflows.  The idea was that when a workflow would start it would look for the existence of a 'parameter.txt', and if it exists in the DBFS location the workflow would start.  If it did not exist then the workflow would simply wait for that file to be created because another workflow dependecy was finishing up.  Once the dependecy finished it would create the 'parameter.txt' file in DBFS and then the downstream workflow would start.

This is how the 'parameter.txt' files are using in 'composite' Informatica workflows (WF) running in Databricks:

- Start WF1 --> WF1 Finishes --> WF1 Creates param1.txt in DBFS --> Start WF2 if 'param1.txt' exists in DBFS or Wait --> WF2 Finishes --> WF2 Creates param2.txt in DBFS --> More WFs continue....<br>

The 'main_step3' notebook is simply used for cleanup purposes to move the Excel workflow template, and clean up files.