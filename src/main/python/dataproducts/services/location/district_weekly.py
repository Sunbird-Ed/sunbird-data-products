"""
Generate district level scans, plays and unique devices on a weekly basis
"""
import json
import sys, time
import os
import pdb
import pandas as pd
import requests

from datetime import date, datetime, timedelta
from pathlib import Path
from string import Template
from azure.common import AzureMissingResourceHttpError

from dataproducts.util.utils import create_json, get_data_from_blob, post_data_to_blob, \
                                    push_metric_event, get_location_info, verify_state_district
from dataproducts.resources.queries import district_devices, district_plays, district_scans

class DistrictWeekly:
    def __init__(self, data_store_location, druid_hostname, execution_date, location_search):
        self.data_store_location = Path(data_store_location)
        self.druid_hostname = druid_hostname
        self.execution_date = execution_date
        self.location_search = location_search
        self.config = {}
        self.druid_url = ""
        self.headers = {}


    def district_devices(self, result_loc_, date_, state_):
        """
        compute unique devices for a state over a week
        :param result_loc_: pathlib.Path object to store resultant CSV at.
        :param date_: datetime object to use for query and path
        :param state_: state to be used in query
        :return: None
        """
        slug_ = result_loc_.name
        start_date = date_ - timedelta(days=7)
        query = Template(district_devices.init())
        query = query.substitute(app=self.config['context']['pdata']['id']['app'],
                                 portal=self.config['context']['pdata']['id']['portal'],
                                 state=state_,
                                 start_date=datetime.strftime(start_date, '%Y-%m-%dT00:00:00+00:00'),
                                 end_date=datetime.strftime(date_, '%Y-%m-%dT00:00:00+00:00'))
        response = requests.request("POST", self.druid_url, data=query, headers=self.headers)
        if response.status_code == 200:
            if len(response.json()) == 0:
                return
            data = []
            for response in response.json():
                data.append(response['event'])
            df = pd.DataFrame(data)
            df['District'] = df.get('District', pd.Series(index=df.index, name='District'))
            df = verify_state_district(result_loc_.parent, state_, df)
            df = df.fillna('Unknown')
            df = df.groupby(['District', "Platform"]).sum().reset_index()
            df.to_csv(result_loc_.parent.joinpath("{}_district_devices.csv".format(slug_)), index=False)
            post_data_to_blob(result_loc_.parent.joinpath("{}_district_devices.csv".format(slug_)), backup=True)
            df['Unique Devices'] = df['Unique Devices'].astype(int)

            df = df.groupby(['District', 'Platform']).sum().reset_index()

            df = df[['District', 'Platform', 'Unique Devices']]
            df.to_csv(result_loc_.joinpath("aggregated_district_unique_devices.csv"), index=False)
        else:
            with open(result_loc_.joinpath('error_log.log'), 'a') as f:
                f.write(state_ + 'devices ' + str(response.status_code) + response.text)
            exit(1)


    def district_plays(self, result_loc_, date_, state_):
        """
        compute content plays per district over the week for the state
        :param result_loc_: pathlib.Path object to store resultant CSV at.
        :param date_: datetime object to pass query and path
        :param query_: json template for druid query
        :param state_: state to be used in query
        :return: None
        """
        slug_ = result_loc_.name
        start_date = date_ - timedelta(days=7)
        query = Template(district_plays.init())
        query = query.substitute(app=self.config['context']['pdata']['id']['app'],
                                 portal=self.config['context']['pdata']['id']['portal'],
                                 state=state_,
                                 start_date=datetime.strftime(start_date, '%Y-%m-%dT00:00:00+00:00'),
                                 end_date=datetime.strftime(date_, '%Y-%m-%dT00:00:00+00:00'))
        response = requests.request("POST", self.druid_url, data=query, headers=self.headers)
        if response.status_code == 200:
            if len(response.json()) == 0:
                return
            data = []
            for response in response.json():
                data.append(response['event'])
            df = pd.DataFrame(data)
            df['District'] = df.get('District', pd.Series(index=df.index, name='District'))
            df = verify_state_district(result_loc_.parent, state_, df)
            df = df.fillna('Unknown')
            df = df.groupby(['District', "Platform"]).sum().reset_index()
            df.to_csv(result_loc_.parent.joinpath("{}_district_plays.csv".format(slug_)), index=False)
            post_data_to_blob(result_loc_.parent.joinpath("{}_district_plays.csv".format(slug_)), backup=True)
            df = df[['District', 'Platform','Number of Content Plays']]
            df.to_csv(result_loc_.joinpath("aggregated_district_content_plays.csv"), index=False)
        else:
            with open(result_loc_.parent.joinpath('error_log.log'), 'a') as f:
                f.write(state_ + 'plays ' + str(response.status_code) + response.text)
            exit(1)


    def district_scans(self, result_loc_, date_, state_):
        """
        compute scans for a district over the week for the state
        :param result_loc_: pathlib.Path object to store resultant CSV at.
        :param date_: datetime object to be passed to the query and path
        :param query_: json template for druid query
        :param state_: state to be used in query
        :return: None
        """
        slug_ = result_loc_.name
        start_date = date_ - timedelta(days=7)
        query = Template(district_scans.init())
        query = query.substitute(app=self.config['context']['pdata']['id']['app'],
                                 portal=self.config['context']['pdata']['id']['portal'],
                                 state=state_,
                                 start_date=datetime.strftime(start_date, '%Y-%m-%dT00:00:00+00:00'),
                                 end_date=datetime.strftime(date_, '%Y-%m-%dT00:00:00+00:00'))
        response = requests.request("POST", self.druid_url, data=query, headers=self.headers)
        if response.status_code == 200:
            if len(response.json()) == 0:
                return
            data = []
            for response in response.json():
                data.append(response['event'])
            df = pd.DataFrame(data)
            df['District'] = df.get('District', pd.Series(index=df.index, name='District'))
            df = verify_state_district(result_loc_.parent, state_, df)
            df = df.fillna('Unknown')
            df = df.groupby(['District', "Platform"]).sum().reset_index()
            df.to_csv(result_loc_.parent.joinpath("{}_district_scans.csv".format(slug_)), index=False)
            post_data_to_blob(result_loc_.parent.joinpath("{}_district_scans.csv".format(slug_)), backup=True)
            df = df[['District', 'Platform', 'Number of QR Scans']]
            df.to_csv(result_loc_.joinpath("aggregated_district_qr_scans.csv"), index=False)
        else:
            with open(result_loc_.parent.joinpath('error_log.log'), 'a') as f:
                f.write(state_ + 'scans ' + str(response.status_code) + response.text)
            exit(1)


    def merge_metrics(self, result_loc_, date_):
        """
        merge all the metrics
        :param result_loc_: pathlib.Path object to store resultant CSV at.
        :param date_: datetime object to be used in path
        :return: None
        """
        slug_ = result_loc_.name
        result_loc_.parent.parent.parent.joinpath("portal_dashboards").mkdir(exist_ok=True)
        last_sunday = datetime.strftime(date_ - timedelta(days=1), '%d/%m/%Y')
        try:
            devices_df = pd.read_csv(
                result_loc_.joinpath("aggregated_district_unique_devices.csv")).set_index(
                ['District', 'Platform'])
        except FileNotFoundError:
            devices_df = pd.DataFrame([], columns=['District', 'Platform', 'Unique Devices']).set_index(
                ['District', 'Platform'])
        try:
            plays_df = pd.read_csv(
                result_loc_.joinpath("aggregated_district_content_plays.csv")).set_index(
                ['District', 'Platform'])
        except FileNotFoundError:
            plays_df = pd.DataFrame([], columns=['District', 'Platform', 'Number of Content Plays']).set_index(
                ['District', 'Platform'])
        try:
            scans_df = pd.read_csv(
                result_loc_.joinpath("aggregated_district_qr_scans.csv")).set_index(
                ['District', 'Platform'])
        except FileNotFoundError:
            scans_df = pd.DataFrame([], columns=['District', 'Platform', 'Number of QR Scans']).set_index(
                ['District', 'Platform'])
        district_df = devices_df.join(scans_df, how='outer').join(plays_df, how='outer').reset_index().pivot(
            index='District', columns='Platform')
        district_df = district_df.join(district_df.sum(level=0, axis=1))
        district_df.columns = [col[0] + ' on ' + col[1].split('.')[-1] if isinstance(col, tuple) else 'Total ' + col for col
                               in district_df.columns]
        district_df['Data as on Last day (Sunday) of the week'] = last_sunday
        district_df = district_df.reset_index()
        district_df.index = [pd.to_datetime(district_df['Data as on Last day (Sunday) of the week'], format='%d/%m/%Y'),
                             district_df['District']]
        for c in ['Unique Devices on portal', 'Unique Devices on app', 'Total Unique Devices',
                  'Number of QR Scans on portal', 'Number of QR Scans on app', 'Total Number of QR Scans',
                  'Number of Content Plays on portal', 'Number of Content Plays on app', 'Total Number of Content Plays']:
            if c not in district_df.columns:
                district_df[c] = 0
        try:
            get_data_from_blob(
                result_loc_.parent.parent.parent.joinpath("portal_dashboards", slug_, "aggregated_district_data.csv"))
            blob_data = pd.read_csv(
                result_loc_.parent.parent.parent.joinpath("portal_dashboards", slug_, "aggregated_district_data.csv"))
            blob_data = blob_data[blob_data['Data as on Last day (Sunday) of the week'] != last_sunday]
            blob_data.index = [pd.to_datetime(blob_data['Data as on Last day (Sunday) of the week'], format='%d/%m/%Y'),
                               blob_data['District']]
        except AzureMissingResourceHttpError:
            blob_data = pd.DataFrame()
        except FileNotFoundError:
            blob_data = pd.DataFrame()
        district_df = pd.concat([blob_data, district_df], sort=True)
        district_df = district_df.sort_index().drop_duplicates(
            subset=['Data as on Last day (Sunday) of the week', 'District'], keep='last').fillna(0)
        district_df = district_df[
            ['Data as on Last day (Sunday) of the week', 'District', 'Unique Devices on app', 'Unique Devices on portal',
             'Total Unique Devices',
             'Number of QR Scans on app', 'Number of QR Scans on portal', 'Total Number of QR Scans',
             'Number of Content Plays on app', 'Number of Content Plays on portal', 'Total Number of Content Plays']]
        district_df.to_csv(
            result_loc_.parent.parent.parent.joinpath("portal_dashboards", slug_, "aggregated_district_data.csv"),
            index=False)
        create_json(result_loc_.parent.parent.parent.joinpath("portal_dashboards", slug_, "aggregated_district_data.csv"))
        post_data_to_blob(
            result_loc_.parent.parent.parent.joinpath("portal_dashboards", slug_, "aggregated_district_data.csv"))


    def init(self):
        start_time_sec = int(round(time.time()))
        file_path = Path(__file__)
        result_loc = self.data_store_location.joinpath('district_reports')
        result_loc.mkdir(exist_ok=True)
        result_loc.parent.joinpath('config').mkdir(exist_ok=True)
        analysis_date = datetime.strptime(self.execution_date, "%d/%m/%Y")
        get_data_from_blob(result_loc.joinpath('slug_state_mapping.csv'))
        get_location_info(result_loc, self.location_search, analysis_date)
        tenant_info = pd.read_csv(result_loc.joinpath('slug_state_mapping.csv'))
        self.druid_url = "{}druid/v2/".format(self.druid_hostname)
        self.headers = {
            'Content-Type': "application/json"
        }
        result_loc.parent.joinpath('config').mkdir(exist_ok=True)
        get_data_from_blob(result_loc.parent.joinpath('config', 'diksha_config.json'))
        with open(result_loc.parent.joinpath('config', 'diksha_config.json'), 'r') as f:
            self.config = json.loads(f.read())
        for ind, row in tenant_info.iterrows():
            state = row['state']
            print(state)
            result_loc.joinpath(analysis_date.strftime('%Y-%m-%d')).mkdir(exist_ok=True)
            result_loc.joinpath(analysis_date.strftime('%Y-%m-%d'), row['slug']).mkdir(exist_ok=True)
            path = result_loc.joinpath(analysis_date.strftime('%Y-%m-%d'), row['slug'])
            if isinstance(state, str):
                self.district_devices(result_loc_=path, date_=analysis_date, state_=state)
                self.district_plays(result_loc_=path, date_=analysis_date, state_=state)
                self.district_scans(result_loc_=path, date_=analysis_date, state_=state)
                self.merge_metrics(result_loc_=path, date_=analysis_date)

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
        push_metric_event(metrics, "District Weekly Report")