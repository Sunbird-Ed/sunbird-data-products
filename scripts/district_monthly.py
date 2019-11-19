"""
Compute unique devices in a district over a month
"""
import argparse
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import requests
from utils import create_json, post_data_to_blob


# TODO: Remove DIKSHA specific filters from query
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
        start_date = datetime(year, month - 1, 1)
    else:
        start_date = datetime(year - 1, 12, 1)
    with open(file_path.parent.joinpath('resources').joinpath(query_)) as f:
        query = f.read()
    query = query.replace('state_name', state_)
    query = query.replace('start_date', start_date.strftime('%Y-%m-%dT00:00:00+00:00'))
    query = query.replace('end_date', date_.strftime('%Y-%m-%dT00:00:00+00:00'))
    response = requests.request("POST", url, data=query, headers=headers)
    if response.status_code == 200:
        if len(response.json()) == 0:
            return
        data = []
        for response in response.json():
            data.append(response['event'])
        df = pd.DataFrame(data).fillna('Unknown')
        df.to_csv(result_loc_.parent.parent.joinpath("district_reports", date_.strftime("%Y-%m-%d)"),
                                                     "{}_monthly.csv".format(slug_)), index=False)
        post_data_to_blob(result_loc_.parent.parent.joinpath("district_reports", date_.strftime("%Y-%m-%d)"),
                                                             "{}_monthly.csv".format(slug_)), backup=True)
        df['Unique Devices'] = df['Unique Devices'].astype(int)
        df = df.join(city_district[city_district['state'] == state_].set_index('City'), on='City', how='left').fillna(
            'Unknown').groupby('District')['Unique Devices'].sum().reset_index()
        df.to_csv(result_loc_.joinpath("aggregated_unique_users_summary.csv"), index=False)
        create_json(result_loc_.joinpath("aggregated_unique_users_summary.csv"))
        post_data_to_blob(result_loc_.joinpath("aggregated_unique_users_summary.csv"))
    else:
        with open(result_loc_.parent.joinpath('error_log.log'), 'a') as f:
            f.write(state_ + 'summary ' + response.status_code + response.text)


parser = argparse.ArgumentParser()
parser.add_argument("data_store_location", type=str, help="data folder location")
parser.add_argument("Druid_hostname", type=str, help="Host address for Druid")
parser.add_argument("-execution_date", type=str, default=date.today().strftime("%d/%m/%Y"),
                    help="DD/MM/YYYY, optional argument for backfill jobs")
args = parser.parse_args()

analysis_date = datetime.strptime(args.execution_date, "%d/%m/%Y")
file_path = Path(__file__)
result_path = Path(args.data_store_location).joinpath('portal_dashboards')
result_path.mkdir(exist_ok=True)
result_path.parent.joinpath("district_reports").mkdir(exist_ok=True)
result_path.parent.joinpath("district_reports", analysis_date.strftime("%Y-%m-%d)")).mkdir(exist_ok=True)
# TODO: Move city-district and state-slug mapping outside
tenant_info = pd.read_csv(file_path.parent.parent.joinpath('resources').joinpath('slug_state_mapping.csv'))
city_district = pd.read_csv(file_path.parent.parent.joinpath('resources').joinpath('city_district_mapping.csv')).fillna(
    'Unknown')
url = "{}druid/v2/".format(args.Druid_hostname)
headers = {
    'Content-Type': "application/json"
}
for ind, row in tenant_info.iterrows():
    print(row['state'])
    result_path.joinpath(row["slug"]).mkdir(exist_ok=True)
    if isinstance(row['state'], str):
        unique_users(result_loc_=result_path.joinpath(row["slug"]), date_=analysis_date,
                     query_='district_devices_monthly.json', state_=row['state'])
