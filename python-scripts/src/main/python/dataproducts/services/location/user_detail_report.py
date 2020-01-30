"""
District wise segragation of User detail report
"""
import json
import sys, time
import os
import pandas as pd
import requests
import pdb
import shutil

from datetime import date, datetime
from pathlib import Path
from string import Template

from dataproducts.util.utils import create_json, get_data_from_blob, \
                            post_data_to_blob, push_metric_event

class UserDetailReport:
    def __init__(self, data_store_location, states):
        self.data_store_location = Path(data_store_location)
        self.states = states


    def init(self):
        for slug in self.states.split(","):
            slug = slug.strip()
            try:
                get_data_from_blob(self.data_store_location.joinpath('location', slug, 'user_detail.csv'))
            except Exception as e:
                print("user_detail.csv not available for "+slug)
                continue
            pdb.set_trace()
            user_df = pd.read_csv(self.data_store_location.joinpath('location', slug, 'user_detail.csv'))
            district_group = user_df.groupby('District name')
            os.makedirs(self.data_store_location.joinpath('location', slug, 'districts'), exist_ok=True)
            for district_name, user_data in user_df.groupby('District name'):
                user_data.to_csv(self.data_store_location.joinpath('location', slug, 'districts', district_name.lower()+".csv"), index=False)

            shutil.make_archive(str(self.data_store_location.joinpath('location', slug, 'districts')),
                                'zip',
                                str(self.data_store_location.joinpath('location', slug, 'districts')))
            post_data_to_blob(self.data_store_location.joinpath('location', slug, 'districts.zip'))