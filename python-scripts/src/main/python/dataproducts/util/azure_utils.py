import os

from pathlib import Path

from azure.storage.blob import BlockBlobService
from azure.common import AzureMissingResourceHttpError

def get_data_from_store(container_name, blob_name, file_path):

    try:
        account_name = os.environ['AZURE_STORAGE_ACCOUNT']
        account_key = os.environ['AZURE_STORAGE_ACCESS_KEY']
        block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)

        block_blob_service.get_blob_to_path(
            container_name=container_name,
            blob_name=blob_name,
            file_path=file_path
            )
    except AzureMissingResourceHttpError:
        raise AzureMissingResourceHttpError("Missing resource!", 404)
    except Exception as e:
        raise Exception('Could not read from blob!'+str(e))


def post_data_to_store(container_name, blob_name, file_path):
    try:
        account_name = os.environ['AZURE_STORAGE_ACCOUNT']
        account_key = os.environ['AZURE_STORAGE_ACCESS_KEY']
        block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)

        block_blob_service.create_blob_from_path(
            container_name=container_name,
            blob_name=blob_name,
            file_path=file_path
            )
    except Exception as e:
        raise Exception('Failed to post to blob!')