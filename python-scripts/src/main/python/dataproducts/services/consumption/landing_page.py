"""
Compute aggregated metrics over academic year
"""
import json
import os, sys, time
import pandas as pd
import pdb

from pathlib import Path
from datetime import datetime, date
from pandas.errors import EmptyDataError

from dataproducts.util.utils import push_metric_event, get_tenant_info, \
                                    download_file_from_store, upload_file_to_store,
                                    file_missing_exception

class LandingPageMetrics:
    def __init__(self, data_store_location, org_search):
        self.data_store_location = Path(data_store_location)
        self.org_search = org_search
        self.current_time = None
        self.private_report_container = os.environ['PRIVATE_REPORT_CONTAINER']
        self.public_report_container = os.environ['PUBLIC_REPORT_CONTAINER']


    def generate_report(self):
        board_slug = pd.read_csv(
                        self.data_store_location.joinpath('textbook_reports', self.current_time.strftime('%Y-%m-%d'), 'tenant_info.csv')
                    )[['id', 'slug']]
        board_slug.set_index('slug', inplace=True)
        result = {}

        for slug, value in board_slug.iterrows():
            try:
                print(slug)
                org_path = self.data_store_location.joinpath('portal_dashboards', slug)
                os.makedirs(org_path, exist_ok=True)

                download_file_from_store(
                    container_name=self.private_report_container,
                    blob_name=org_path.name + '/daily_metrics.csv',
                    file_path=org_path.joinpath('daily_metrics.csv'),
                    is_private=True
                    )
                download_file_from_store(
                    container_name=self.private_report_container,
                    blob_name=org_path.name + '/DCE_textbook_data.csv',
                    file_path=org_path.joinpath('DCE_textbook_data.csv'),
                    is_private=True
                    )
                download_file_from_store(
                    container_name=self.private_report_container,
                    blob_name=org_path.name + '/content_creation.csv',
                    file_path=org_path.joinpath('content_creation.csv'),
                    is_private=True
                    )
                dm_df = pd.read_csv(org_path.joinpath('daily_metrics.csv'))
                dm_df = dm_df.set_index('Date')
                dm_df.set_index(pd.to_datetime(dm_df.index, format='%d-%m-%Y'), inplace=True)
                _2018 = dm_df.loc[dm_df.index < '2019-06-01'].sum()[
                    ['Total QR scans', 'Total Content Downloads', 'Total Content Plays', 'Total Content Play Time (in hours)']]
                _2019 = dm_df.loc[(dm_df.index >= '2019-06-01') & (dm_df.index < '2020-06-01')].sum()[
                    ['Total QR scans', 'Total Content Downloads', 'Total Content Plays', 'Total Content Play Time (in hours)']]
                _2020 = dm_df.loc[dm_df.index >= '2020-06-01'].sum()[
                    ['Total QR scans', 'Total Content Downloads', 'Total Content Plays', 'Total Content Play Time (in hours)']]
                _2018.to_json(org_path.joinpath('landing_page_2018.json'))
                _2019.to_json(org_path.joinpath('landing_page_2019.json'))
                _2020.to_json(org_path.joinpath('landing_page_2020.json'))
                try:
                    dce_df = pd.read_csv(org_path.joinpath('DCE_textbook_data.csv'))
                    cc_df = pd.read_csv(org_path.joinpath('content_creation.csv'))
                    result = {
                        'no_of_textbooks': dce_df.shape[0],
                        'no_of_qr_codes': int(dce_df['Total number of QR codes'].sum()),
                        'no_of_resource': cc_df[cc_df['Status'] == 'live']['Status from the beginning'].values.tolist()[0]
                    }
                except:
                    result = {
                        'no_of_textbooks': 0,
                        'no_of_qr_codes': 0,
                        'no_of_resource': 0
                    }
                with open(str(org_path.joinpath('landing_page_creation_metrics.json')), 'w') as f:
                    json.dump(result, f)

                upload_file_to_store(
                    container_name=self.public_report_container,
                    blob_name=org_path.name + '/landing_page_2018.json',
                    file_path=str(org_path.joinpath('landing_page_2018.json')),
                    is_private=False
                    )
                upload_file_to_store(
                    container_name=self.public_report_container,
                    blob_name=org_path.name + '/landing_page_2019.json',
                    file_path=str(org_path.joinpath('landing_page_2019.json')),
                    is_private=False
                    )
                upload_file_to_store(
                    container_name=self.public_report_container,
                    blob_name=org_path.name + '/landing_page_2020.json',
                    file_path=str(org_path.joinpath('landing_page_2020.json')),
                    is_private=False
                    )
                upload_file_to_store(
                    container_name=self.public_report_container,
                    blob_name=org_path.name + '/landing_page_creation_metrics.json',
                    file_path=str(org_path.joinpath('landing_page_creation_metrics.json')),
                    is_private=False
                    )
            except EmptyDataError:
                pass
            except file_missing_exception():
                pass


    def init(self):
        start_time_sec = int(round(time.time()))
        print('[START] Landing Page!')
        self.current_time = date.today()
        get_tenant_info(result_loc_=self.data_store_location.joinpath('textbook_reports'), org_search_=self.org_search,
                        date_=self.current_time)

        self.generate_report()
        print('[SUCCESS] Landing Page!')

        end_time_sec = int(round(time.time()))
        time_taken = end_time_sec - start_time_sec
        metrics = [
            {
                "metric": "timeTakenSecs",
                "value": time_taken
            },
            {
                "metric": "date",
                "value": self.current_time.strftime("%Y-%m-%d")
            }
        ]
        push_metric_event(metrics, "Landing Page")