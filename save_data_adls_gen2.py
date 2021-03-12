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
parser = argparse.ArgumentParser("Save Data to ADLS Gen2 Args")
parser.add_argument("--processed_dataset", dest='processed_dataset', required=True)
parser.add_argument("--processed_dataset_interim", dest='processed_dataset_interim', required=True)
parser.add_argument('--query_param', type=int, required=True)
args, _ = parser.parse_known_args()
query_param = args.query_param
processed_dataset = args.processed_dataset
processed_dataset_interim = args.processed_dataset_interim

#Get current run
current_run = Run.get_context()

#Parse intermediate data into pandas dataframe
processed_df = pd.read_csv(processed_dataset_interim)





#########################MODIFY###########################



filename = '{}_filename.csv'.format(query_param)



##########################################################





#Save data to ADLS Gen 2
os.makedirs(processed_dataset, exist_ok=True)
processed_df.to_csv(os.path.join(processed_dataset, filename), index=False)