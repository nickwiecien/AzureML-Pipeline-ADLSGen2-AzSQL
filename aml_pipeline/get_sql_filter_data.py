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
parser = argparse.ArgumentParser("Get Filter SQL Data")
parser.add_argument('--query_param', type=int, required=True)
parser.add_argument("--filter_dataset", dest='filter_dataset', required=True)
args, _ = parser.parse_known_args()
query_param = args.query_param
filter_dataset = args.filter_dataset

#Get current run
current_run = Run.get_context()

#Get associated AML workspace
ws = current_run.experiment.workspace




#########################MODIFY###########################



#Get Azure SQL Datastore - CHANGE AZURE SQL DATASTORE NAME
azsql_ds = Datastore.get(ws, 'azsql_ds')

#UPDATE QUERY STRING HERE 
query_string = 'SELECT * FROM Filter WHERE D={}'.format(str(query_param))



##########################################################





#Query Azure SQL Datastore
filter_sql_query = DataPath(azsql_ds, query_string)
filter_sql_ds = Dataset.Tabular.from_sql_query(filter_sql_query, query_timeout=10)

#Convert dataset to pandas dataframe
filter_df = filter_sql_ds.to_pandas_dataframe()

#Write dataframe to output dataset path
os.makedirs(filter_dataset, exist_ok=True)
filter_df.to_csv(os.path.join(filter_dataset, 'filter_data.csv'), index=False)
