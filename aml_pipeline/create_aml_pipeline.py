# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %% [markdown]
# # Azure ML Pipeline - ADLS Gen2/Azure SQL DB
# This notebook demonstrates creation & execution of an Azure ML pipeline designed to query data from an attached Azure SQL Datastore (using dynamic arguments passed as a pipeline parameter), process that data, and then export the result dataset to ADLS Gen 2 and the attached Azure SQL DB.
# %% [markdown]
# ### Import required packages

# %%
from azureml.core import Workspace, Experiment, Datastore, Environment, Dataset
from azureml.core.compute import ComputeTarget, AmlCompute, DataFactoryCompute
from azureml.core.compute_target import ComputeTargetException
from azureml.core.runconfig import RunConfiguration
from azureml.core.conda_dependencies import CondaDependencies
from azureml.core.runconfig import DEFAULT_CPU_IMAGE
from azureml.pipeline.core import Pipeline, PipelineParameter, PipelineData
from azureml.pipeline.steps import PythonScriptStep
from azureml.pipeline.core import PipelineParameter, PipelineData
from azureml.data.output_dataset_config import OutputTabularDatasetConfig, OutputDatasetConfig, OutputFileDatasetConfig
from azureml.data.datapath import DataPath
from azureml.data.data_reference import DataReference
from azureml.data.sql_data_reference import SqlDataReference
from azureml.pipeline.steps import DataTransferStep
import logging

from aml_pipeline.env_variables import Env

def main():
    e = Env()

    #Connect to AML Workspace
    ws = Workspace.get(
        name=e.workspace_name,
        subscription_id=e.subscription_id,
        resource_group=e.resource_group,
    )

    #Select AML Compute Cluster
    cpu_cluster_name = e.compute_name

    # Verify that cluster does not exist already
    try:
        cpu_cluster = ComputeTarget(workspace=ws, name=cpu_cluster_name)
        print('Found an existing cluster, using it instead.')
    except ComputeTargetException:
        compute_config = AmlCompute.provisioning_configuration(vm_size=e.vm_size,
                                                            min_nodes=e.min_nodes,
                                                            max_nodes=e.max_nodes)
        cpu_cluster = ComputeTarget.create(ws, cpu_cluster_name, compute_config)
        cpu_cluster.wait_for_completion(show_output=True)
        
    #Create Data Factory Compute for DataTransferStep
    def get_or_create_data_factory(workspace, factory_name):
        try:
            return DataFactoryCompute(workspace, factory_name)
        except ComputeTargetException as e:
            if 'ComputeTargetNotFound' in e.message:
                print('Data factory not found, creating...')
                provisioning_config = DataFactoryCompute.provisioning_configuration()
                data_factory = ComputeTarget.create(workspace, factory_name, provisioning_config)
                data_factory.wait_for_completion()
                return data_factory
            else:
                raise e
    data_factory_name = e.data_factory_name           
    data_factory_compute = get_or_create_data_factory(ws, data_factory_name)

    #Get Default, Azure SQL, and ADLS Gen2 Datastores
    default_ds = ws.get_default_datastore()
    azsql_ds = Datastore.get(ws, e.sql_datastore_name)
    adlsgen2_ds = Datastore.get(ws, e.adls_datastore_name)

    #Create run configuration
    run_config = RunConfiguration()
    run_config.environment.docker.enabled = True
    run_config.environment.docker.base_image = DEFAULT_CPU_IMAGE

    #Define intermediate/output datasets
    profile_dataset = OutputFileDatasetConfig(name='profile_data', destination=(default_ds, 'profile_data/{run-id}')).read_delimited_files().register_on_complete(name='profile_data')
    filter_dataset = OutputFileDatasetConfig(name='filter_data', destination=(default_ds, 'filter_data/{run-id}')).read_delimited_files().register_on_complete(name='filter_data')
    processed_dataset_sql = PipelineData('processed_data_sql', datastore=default_ds)
    processed_dataset_adls = OutputFileDatasetConfig(name='processed_data_adls', destination=(adlsgen2_ds, '{run-id}')).read_delimited_files().register_on_complete(name='processed_data_adls')
    sql_data_ref = SqlDataReference(datastore=azsql_ds, sql_table="Results", data_reference_name='azure_sql_reference')

    #Define pipeline parameters
    project_id = PipelineParameter(name='project_id', default_value=101)
    output_path = PipelineParameter(name='output_path', default_value='processed_data')

    #Define pipeline steps
    get_filter_data_step = PythonScriptStep(
        name='get-filter-data',
        script_name='get_sql_filter_data.py',
        arguments =['--project_id', project_id,
                '--filter_dataset', filter_dataset],
        outputs=[filter_dataset],
        compute_target=cpu_cluster,
        source_directory=e.source_directory,
        allow_reuse=False,
        runconfig=run_config
    )

    #SQL query to get 'profile' data.
    #Data is collected in the profile_dataset, registered upon step completion, and passed to subsequent steps.
    get_profile_data_step = PythonScriptStep(
        name='get-profile-data',
        script_name='get_sql_profile_data.py',
        arguments =['--profile_dataset', profile_dataset],
        outputs=[profile_dataset],
        compute_target=cpu_cluster,
        source_directory=e.source_directory,
        allow_reuse=False,
        runconfig=run_config
    )

    #This step should contain all of the ML goodness.
    #filter_dataset & profile_dataset are passed in and parsed into pandas dataframes.
    #Output SQL data is collected in the processed_dataset_sql PipelineData object
    #which is subsequently inserted Azure SQL DB.
    #Output ADLS Gen 2 data is collected in processed_dataset_adls and registered upon completion.
    process_data_step = PythonScriptStep(
        name='process-data',
        script_name='process_data.py',
        arguments =['--processed_dataset_sql', processed_dataset_sql, '--processed_dataset_adls', processed_dataset_adls, '--output_path', output_path],
        inputs=[filter_dataset.as_input('filter_data'), profile_dataset.as_input('profile_data')],
        outputs=[processed_dataset_sql, processed_dataset_adls],
        compute_target=cpu_cluster,
        source_directory=e.source_directory,
        allow_reuse=False,
        runconfig=run_config
    )

    #Transfer processed results from blob storage to Azure SQL DB.
    #processed_data_interim is passed as a reference and inserted into Azure SQL DB.
    #Note: data is inserted into the table specified in the SqlDataReference above.
    transfer_data_to_sql = DataTransferStep(
        name='save-data-sql',
        source_data_reference=processed_dataset_sql,
        destination_data_reference=sql_data_ref,
        compute_target=data_factory_compute)

    #Create pipeline
    pipeline = Pipeline(workspace=ws, steps=[get_filter_data_step, get_profile_data_step, process_data_step, transfer_data_to_sql])

    #Publish pipeline
    published_pipeline = pipeline.publish(name = e.pipeline_name,
                                     description = 'Sample pipeline that queries Azure SQL DB, processes data, and exports results to ADLS Gen2/Azure SQL DB',
                                     continue_on_step_failure = False)

if __name__ == "__main__":
    main()

