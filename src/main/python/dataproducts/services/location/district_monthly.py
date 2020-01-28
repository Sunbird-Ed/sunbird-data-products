"""
Compute unique devices in a district over a month
"""
import json
import sys, time
import os
import pandas as pd
import requests

from datetime import date, datetime
from pathlib import Path
from string import Template

from dataproducts.util.utils import create_json, get_data_from_blob, \
                            post_data_to_blob, push_metric_event, get_location_info
from dataproducts.resources.queries import district_devices_monthly


class DistrictMonthly:

    def __init__(self, data_store_location, druid_hostname, execution_date=date.today().strftime("%d/%m/%Y")):
        self.data_store_location = Path(data_store_location)
        self.druid_hostname = druid_hostname
        self.execution_date = execution_date
        self.config = {}


    def unique_users(self, result_loc_, date_, state_):
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
        query = Template(district_devices_monthly.init())
        query = query.substitute(app=self.config['context']['pdata']['id']['app'],
                                 portal=self.config['context']['pdata']['id']['portal'],
                                 state=state_,
                                 start_date=start_date.strftime('%Y-%m-%dT00:00:00+00:00'),
                                 end_date=date_.strftime('%Y-%m-%dT00:00:00+00:00'))
        url = "{}druid/v2/".format(self.druid_hostname)
        headers = {
            'Content-Type': "application/json"
        }
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


    def init(self):
        start_time_sec = int(round(time.time()))

        analysis_date = datetime.strptime(self.execution_date, "%d/%m/%Y")
        result_loc = self.data_store_location.joinpath('district_reports')
        result_loc.mkdir(exist_ok=True)
        result_loc.joinpath(analysis_date.strftime("%Y-%m-%d")).mkdir(exist_ok=True)
        self.data_store_location.joinpath('config').mkdir(exist_ok=True)
        get_data_from_blob(result_loc.joinpath('slug_state_mapping.csv'))
        tenant_info = pd.read_csv(result_loc.joinpath('slug_state_mapping.csv'))
        get_location_info(result_loc_=data_store_location.parent.joinpath('portal_dashboards'), location_search_=self.org_search,
                        date_=analysis_date)
        get_data_from_blob(self.data_store_location.joinpath('config', 'diksha_config.json'))
        with open(self.data_store_location.joinpath('config', 'diksha_config.json'), 'r') as f:
            self.config = json.loads(f.read())
        for ind, row in tenant_info.iterrows():
            print(row['state'])
            result_loc.joinpath(row["slug"]).mkdir(exist_ok=True)
            if isinstance(row['state'], str):
                self.unique_users(result_loc_=result_loc.joinpath(row["slug"]), date_=analysis_date,
                             state_=row['state'])

        end_time_sec = int(round(time.time()))
        time_taken = end_time_sec - start_time_sec
        metrics = [
            {
                "metric": "timeTakenSecs",
                "value": time_taken
            },
            {
                "metric": "date",
                "value": analysis_date.strftime("%Y-%m-%d")
            }
        ]
        push_metric_event(metrics, "District Monthly Report")