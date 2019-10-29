import json
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from azure.storage.blob import BlockBlobService

def create_json(read_loc_, last_update=False):
    try:
        df = pd.read_csv(read_loc_).fillna('')
        if last_update:
            try:
                _lastUpdateOn = datetime.now().timestamp() * 1000
            except ValueError:
                return
            df = df.astype('str')
            json_file = {
                'keys': df.columns.values.tolist(),
                'data': json.loads(df.to_json(orient='records')),
                'tableData': df.values.tolist(),
                'metadata': {
                    'lastUpdatedOn': _lastUpdateOn
                }
            }
        else:
            df = df.astype('str')
            json_file = {
                'keys': df.columns.values.tolist(),
                'data': json.loads(df.to_json(orient='records')),
                'tableData': df.values.tolist()}
        with open(str(read_loc_).split('.csv')[0] + '.json', 'w') as f:
            json.dump(json_file, f)
    except Exception:
        raise Exception('Failed to create JSON!')


def write_data_to_blob(read_loc, file_name):
    account_name = os.environ['AZURE_STORAGE_ACCOUNT_NEW']
    account_key = os.environ['AZURE_STORAGE_ACCESS_KEY_NEW']
    block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)

    container_name = 'reports'

    local_file = file_name

    full_path = os.path.join(read_loc, local_file)

    block_blob_service.create_blob_from_path(container_name, local_file, full_path)

def get_data_from_blob(slug, filename):
    try:
        container_name = 'reports'
        block_blob_service.get_blob_to_path(
          container_name=container_name,
          blob_name=slug + '/' + filename,
          file_path=str(write_path.joinpath('reports', slug, filename))
        )
    except Exception:
        print('Failed to read from blob!'+filename)