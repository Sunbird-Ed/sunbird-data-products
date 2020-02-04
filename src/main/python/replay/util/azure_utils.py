import os
import json
from pathlib import Path

#del os.environ['PYSPARK_SUBMIT_ARGS']
account_name = os.environ['AZURE_STORAGE_ACCOUNT']
account_key = os.environ['AZURE_STORAGE_ACCESS_KEY']

# copy the files to backup folder
def copy_data(container, prefix, destination_path, date):
    command = 'az storage blob copy start-batch --connection-string "AccountName={};AccountKey={};EndpointSuffix=core.windows.net;DefaultEndpointsProtocol=https;" --source-container {} --destination-container {} --destination-path {} --pattern {}/{}*.gz'.format(account_name, account_key, container, container, destination_path, prefix, date.strftime('%Y-%m-%d'))
    stream = os.popen(command)
    output = stream.read()
    output

# delete files
def delete_data(container, prefix, date):
    command = 'az storage blob delete-batch -s {} --connection-string "AccountName={};AccountKey={};EndpointSuffix=core.windows.net;DefaultEndpointsProtocol=https;" --pattern {}/{}*.gz'.format(container, account_name, account_key, prefix, date.strftime('%Y-%m-%d'))
    os.system(command)

# read and return data
def get_data_path(container, prefix, date):
    path = 'wasbs://{}@{}.blob.core.windows.net/{}/{}-*'.format(container, account_name, prefix, date.strftime('%Y-%m-%d'))
    return path
