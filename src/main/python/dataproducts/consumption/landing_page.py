"""
Compute aggregated metrics over academic year
"""
import json
import os, sys, time
from pathlib import Path
from datetime import datetime, date
import argparse
import pandas as pd
from azure.storage.blob import BlockBlobService
from pandas.errors import EmptyDataError

util_path = os.path.abspath(os.path.join(__file__, '..', '..', '..', 'util'))
sys.path.append(util_path)

from utils import push_metric_event

def post_data_to_blob(result_loc, slug):
    """
    for channel, create the two academic years' aggregated data
    :param result_loc: pathlib.Path object to store resultant CSV at.
    :param slug: slug to channel id
    :return: None
    """
    try:
        account_name = os.environ['AZURE_STORAGE_ACCOUNT_NEW']
        account_key = os.environ['AZURE_STORAGE_ACCESS_KEY_NEW']
        block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)
        container_name = 'reports'
        block_blob_service.create_blob_from_path(
            container_name=container_name,
            blob_name=slug + '/landing_page_2018.json',
            file_path=str(result_loc.joinpath(slug, 'landing_page_2018.json'))
        )
        block_blob_service.create_blob_from_path(
            container_name=container_name,
            blob_name=slug + '/landing_page_2019.json',
            file_path=str(result_loc.joinpath(slug, 'landing_page_2019.json'))
        )
        block_blob_service.create_blob_from_path(
            container_name=container_name,
            blob_name=slug + '/landing_page_creation_metrics.json',
            file_path=str(result_loc.joinpath(slug, 'landing_page_creation_metrics.json'))
        )
    except Exception:
        raise Exception('Failed to post to blob!')


start_time_sec = int(round(time.time()))
parser = argparse.ArgumentParser()
parser.add_argument("data_store_location", type=str, help="the path to local data folder")
args = parser.parse_args()
data_store_location = Path(args.data_store_location)
result = {}

for file_path in data_store_location.glob('portal_dashboards/*/daily_metrics.csv'):
    try:
        df = pd.read_csv(file_path)
        df = df.set_index('Date')
        df.set_index(pd.to_datetime(df.index, format='%d-%m-%Y'), inplace=True)
        _2018 = df.loc[df.index < '2019-06-01'].sum()[
            ['Total QR scans', 'Total Content Downloads', 'Total Content Plays', 'Total Content Play Time (in hours)']]
        _2019 = df.loc[df.index >= '2019-06-01'].sum()[
            ['Total QR scans', 'Total Content Downloads', 'Total Content Plays', 'Total Content Play Time (in hours)']]
        _2018.to_json(file_path.parent.joinpath('landing_page_2018.json'))
        _2019.to_json(file_path.parent.joinpath('landing_page_2019.json'))
        try:
            df1 = pd.read_csv(file_path.parent.joinpath('DCException_Report_Textbook_Level.csv'))
            df2 = pd.read_csv(file_path.parent.joinpath('content_creation.csv'))
            result = {
                'no_of_textbooks': df1.shape[0],
                'no_of_qr_codes': int(df1['Total number of QR codes'].sum()),
                'no_of_resource': df2[df2['Status'] == 'live']['Status from the beginning'].values.tolist()[0]
            }
        except:
            result = {
                'no_of_textbooks': 0,
                'no_of_qr_codes': 0,
                'no_of_resource': 0
            }
        with open(str(file_path.parent.joinpath('landing_page_creation_metrics.json')), 'w') as f:
            json.dump(result, f)
        post_data_to_blob(file_path.parent.parent, str(file_path.parent.name))
    except EmptyDataError:
        pass
print('[SUCCESS] Landing Page!')

end_time_sec = int(round(time.time()))
time_taken = end_time_sec - start_time_sec
metrics = {
    "system": "AdhocJob",
    "subsystem": "Landing Page",
    "metrics": [
        {
            "metric": "timeTakenSecs",
            "value": time_taken
        },
        {
            "metric": "date",
            "value": date.today().strftime("%Y-%m-%d")
        }
    ]
}
push_metric_event(metrics, "Landing Page")