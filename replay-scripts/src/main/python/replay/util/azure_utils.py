import os
import json
from pathlib import Path
from azure.storage.blob import BlockBlobService

#del os.environ['PYSPARK_SUBMIT_ARGS']
account_name = os.environ['azure_storage_key']
account_key = os.environ['azure_storage_secret']
block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)

def listBlobs(container, prefix, date):
    key = '{}/{}'.format(prefix, date.strftime('%Y-%m-%d'))
    return list(block_blob_service.list_blob_names(container, key))

# copy the files to backup folder
def copy_data(container, prefix, destination_path, date):
    filesList = listBlobs(container, prefix, date)
    for file in filesList:
        source = "https://{}.blob.core.windows.net/{}/{}".format(account_name, container, file)
        dest = file.split('/')[1]
        block_blob_service.copy_blob(container, '{}/{}'.format(destination_path, dest), source)

# delete files
def delete_data(container, prefix, date):
    command = 'az storage blob delete-batch -s {} --connection-string "AccountName={};AccountKey={};EndpointSuffix=core.windows.net;DefaultEndpointsProtocol=https;" --pattern {}/{}*.gz'.format(container, account_name, account_key, prefix, date.strftime('%Y-%m-%d'))
    os.system(command)

# read and return data
def get_data_path(container, prefix, date):
    path = 'wasbs://{}@{}.blob.core.windows.net/{}/{}-*'.format(container, account_name, prefix, date.strftime('%Y-%m-%d'))
    return path
