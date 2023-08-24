# Automated Databricks Workflow Creation Tool

Automating the creation of 'simple' and 'complex' Databricks workflows can help with Databrick migrations from Informatica, Oracle, and SQL Server.  Creating and wiring workflows together that execute notebooks to orchestrate the creation, deletion, and updates of data across Bronze, Silver and Gold can be complex and time consuming.  This tool was created originally to help with Informatica Workflow migrations specifically but it has a much wider use with generic Databricks workflow creation.

### Link to Github Repo: https://github.com/robert-altmiller/workflow_automation_optimization

## Features of the Databricks Workflow Creation Tool

The Databricks Workflow Creation Tool has the following features:
- Create workflows that can run Notebooks in Github or in the local Databricks workspace.
- Create and add custom workflow parameters.
- Apply group permissions to workflows.
- Boolean flags enable / disable the creation of duplicate workflows.
- Code Export the workflow configuration in JSON format.
- Workflow creation input parameters are injected using Excel template.
- Email alerts are automatically set on workflows if they fail.
- Workflow Schedule is set in Excel template.
- Workflow cluster is auto assigned using Databricks widgets.


## How to Use the Databricks Workflow Creation Tool