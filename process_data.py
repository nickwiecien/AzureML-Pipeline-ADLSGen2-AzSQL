# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

from azureml.core import Run, Workspace, Datastore, Dataset
from azureml.data.datapath import DataPath
import pandas as pd
import numpy as np
import os
import argparse
import datetime
import json

#Parse input arguments
parser = argparse.ArgumentParser("Process Filter/Profile Data")
parser.add_argument("--processed_dataset_adls", dest='processed_dataset_adls', required=True)
parser.add_argument("--processed_dataset_sql", dest='processed_dataset_sql', required=True)
parser.add_argument("--output_path", type=str, required=True)
args, _ = parser.parse_known_args()
processed_dataset_sql = args.processed_dataset_sql
processed_dataset_adls = args.processed_dataset_adls
output_path = args.output_path

#Get current run
current_run = Run.get_context()

#Parse profile and filter datasets to pandas dataframes
profile_dataset = current_run.input_datasets['profile_data']
profile_df = profile_dataset.to_pandas_dataframe()
filter_dataset = current_run.input_datasets['filter_data']
filter_df = filter_dataset.to_pandas_dataframe()




#########################MODIFY###########################



#Datasets were merged here as a representative example.
#Replace this with ML code.
merged_df = pd.concat([profile_df, filter_df], axis=1)



##########################################################




#Write sql dataframe to processed_dataset_sql path - this will be
#inserted into SQL via the DataTransferStep
merged_df.to_csv(processed_dataset_sql, index=False)

#Save adls dataframe to processed_dataset_adls path
os.makedirs(processed_dataset_adls, exist_ok=True)
filename = '{}.csv'.format(output_path)
merged_df.to_csv(os.path.join(processed_dataset_adls, filename), index=False)