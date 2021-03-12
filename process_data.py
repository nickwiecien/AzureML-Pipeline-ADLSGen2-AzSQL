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
parser.add_argument("--processed_dataset_interim", dest='processed_dataset_interim', required=True)
args, _ = parser.parse_known_args()
processed_dataset_interim = args.processed_dataset_interim

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




#Write final dataframe to output dataset path
merged_df.to_csv(processed_dataset_interim, index=False)