"""
Compute unique devices in a district over a month
"""
import json
import sys, time
import os
from datetime import date, datetime
from pathlib import Path
from string import Template

import argparse
import pandas as pd
import requests

util_path = os.path.abspath(os.path.join(__file__, '..', '..', '..', 'util'))
sys.path.append(util_path)

from utils import create_json, get_data_from_blob, post_data_to_blob, push_metric_event


def unique_users(result_loc_, date_, query_, state_):
    """
    Query druid for unique users by district over a month for a state
    :param result_loc_: pathlib.Path object to store resultant CSV
    :param date_: datetime object to pass for query and path
    :param query_: json query template
    :param state_: the state to be used in query
    :return: None
    """
    slug_ = result_loc_.name
    year = date_.year
    month = date_.month
    if month != 1:
        start_date = datetime(year, 11, 1)
    else:
        start_date = datetime(year - 1, 12, 1)
    with open(file_path.parent.parent.parent.parent.parent.parent.joinpath('resources', 'queries').joinpath(query_)) as f:
        query = Template(f.read())
    query = query.substitute(app=config['context']['pdata']['id']['app'],
                             portal=config['context']['pdata']['id']['portal'],
                             state=state_,
                             start_date=start_date.strftime('%Y-%m-%dT00:00:00+00:00'),
                             end_date=date_.strftime('%Y-%m-%dT00:00:00+00:00'))
    response = requests.request("POST", url, data=query, headers=headers)
    if response.status_code == 200:
        if len(response.json()) == 0:
            return
        data = []
        for response in response.json():
            data.append(response['event'])
        df = pd.DataFrame(data).fillna('Unknown')
        df.to_csv(result_loc_.parent.joinpath(date_.strftime("%Y-%m-%d"),
                                                     "{}_monthly.csv".format(slug_)), index=False)
        post_data_to_blob(result_loc_.parent.joinpath(date_.strftime("%Y-%m-%d"),
                                                             "{}_monthly.csv".format(slug_)), backup=True)
        df['Unique Devices'] = df['Unique Devices'].astype(int)
        df = df[['District', 'Unique Devices']]
        df.to_csv(result_loc_.joinpath("aggregated_unique_users_summary.csv"), index=False)
        create_json(result_loc_.joinpath("aggregated_unique_users_summary.csv"))
        post_data_to_blob(result_loc_.joinpath("aggregated_unique_users_summary.csv"))
    else:
        with open(result_loc_.parent.joinpath('error_log.log'), 'a') as f:
            f.write(state_ + 'summary ' + response.status_code + response.text)


start_time_sec = int(round(time.time()))
parser = argparse.ArgumentParser()
parser.add_argument("--data_store_location", type=str, help="data folder location")
parser.add_argument("--druid_hostname", type=str, help="Host address for Druid")
parser.add_argument("--execution_date", type=str, default=date.today().strftime("%d/%m/%Y"),
                    help="DD/MM/YYYY, optional argument for backfill jobs")
args = parser.parse_args()

analysis_date = datetime.strptime(args.execution_date, "%d/%m/%Y")
file_path = Path(__file__)
result_loc = Path(args.data_store_location).joinpath('district_reports')
result_loc.mkdir(exist_ok=True)
result_loc.joinpath(analysis_date.strftime("%Y-%m-%d")).mkdir(exist_ok=True)
result_loc.parent.joinpath('config').mkdir(exist_ok=True)
get_data_from_blob(result_loc.joinpath('slug_state_mapping.csv'))
tenant_info = pd.read_csv(result_loc.joinpath('slug_state_mapping.csv'))
url = "{}druid/v2/".format(args.druid_hostname)
headers = {
    'Content-Type': "application/json"
}
get_data_from_blob(result_loc.parent.joinpath('config', 'diksha_config.json'))
with open(result_loc.parent.joinpath('config', 'diksha_config.json'), 'r') as f:
    config = json.loads(f.read())
for ind, row in tenant_info.iterrows():
    print(row['state'])
    result_loc.joinpath(row["slug"]).mkdir(exist_ok=True)
    if isinstance(row['state'], str):
        unique_users(result_loc_=result_loc.joinpath(row["slug"]), date_=analysis_date,
                     query_='district_devices_monthly.json', state_=row['state'])

end_time_sec = int(round(time.time()))
time_taken = end_time_sec - start_time_sec
metrics = [
    {
        "metric": "timeTakenSecs",
        "value": time_taken
    },
    {
        "metric": "date",
        "value": datetime.strptime(args.execution_date, "%Y-%m-%d")
    }
]
push_metric_event(metrics, "District Monthly Report")