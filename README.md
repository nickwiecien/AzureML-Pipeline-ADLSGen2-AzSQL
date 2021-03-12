# AzureML-Pipeline-ADLSGen2-AzSQL

Sample Azure Machine Learning pipeline demonstrating how to query Azure SQL DB using variable pipeline arguments, register datasets between pipeline steps, and sink ML-processed result data into Azure SQL DB & ADLS Gen2. 

This pipeline accepts one variable argument in the form of a `PipelineParameter` which is used to dynamically modify SQL queries and update filenames when exporting data to ADLS Gen2.

![AML Pipeline](img/aml_pipeline.PNG?raw=true "AzureML-Pipeline-ADLSGen2-AzSQL")

## Environment Setup
<b>Note:</b> Recommend running this notebook using an Azure Machine Learning compute instance using the preconfigured `Python 3.6 - AzureML` environment.

To build and run the sample pipeline contained in `SamplePipeline.ipynb` the following resources are required:
* Azure Machine Learning Workspace
* ADLS Gen 2 Account registered as a datastore in the AML Workspace
    * Named `azadlsgen2_ds` in this example
    * Authenticated using a service principal that has <b>Storage Blob Data Contributor</b> access
* Azure SQL DB registerd as a datastore in the AML Workspace
    * Named `azsql_ds` in this example
    * Authenticated using a servince principal - see [this document](https://docs.microsoft.com/en-us/python/api/azureml-pipeline-steps/azureml.pipeline.steps.datatransferstep?view=azure-ml-py#remarks) and [this document](https://docs.microsoft.com/en-us/azure/data-factory/connector-azure-sql-database#service-principal-authentication) for details on configuring servince principal access
    * Sql database contains three tables `Filter`, `Profile`, and `Results`
        * Tables can be created by running the `CreateTables.sql` script
        * Data contained in `Filter_Data.csv` was uploaded to `Filter` table
        * Data contained in `Profile_Data.csv` was uploaded to `Profile` table
        * Processed results from the pipeline run are written into `Results` table

