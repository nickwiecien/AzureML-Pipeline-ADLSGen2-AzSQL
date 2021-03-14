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
parser = argparse.ArgumentParser("Get Profile SQL Data")
parser.add_argument("--profile_dataset", dest='profile_dataset', required=True)
args, _ = parser.parse_known_args()
profile_dataset = args.profile_dataset

#Get current run
current_run = Run.get_context()

#Get associated AML workspace
ws = current_run.experiment.workspace





#########################MODIFY###########################


#Get Azure SQL Datastore - CHANGE AZURE SQL DATASTORE NAME
azsql_ds = Datastore.get(ws, 'azsql_ds')

#UPDATE QUERY STRING HERE
query_string = 'SELECT * FROM Profile'


##########################################################





#Query Azure SQL Datastore
profile_sql_query = DataPath(azsql_ds, query_string)
profile_sql_ds = Dataset.Tabular.from_sql_query(profile_sql_query, query_timeout=10)

#Convert dataset to pandas dataframe and return
profile_df = profile_sql_ds.to_pandas_dataframe()

#Write dataframe to output dataset path
os.makedirs(profile_dataset, exist_ok=True)
profile_df.to_csv(os.path.join(profile_dataset, 'profile_data.csv'), index=False)