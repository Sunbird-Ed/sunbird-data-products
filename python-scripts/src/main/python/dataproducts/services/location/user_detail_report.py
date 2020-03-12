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
        result_loc = self.data_store_location.joinpath('location')
        for slug in self.states.split(","):
            slug = slug.strip()
            try:
                get_data_from_blob(result_loc.joinpath(slug, 'validated-user-detail.csv'))
            except Exception as e:
                print("validated-user-detail.csv not available for "+slug)
                continue
            try:
                get_data_from_blob(result_loc.joinpath(slug, 'validated-user-detail-state.csv'))
            except Exception as e:
                print("validated-user-detail-state.csv not available for "+slug)
            user_df = pd.read_csv(result_loc.joinpath(slug, 'validated-user-detail.csv'))
            district_group = user_df.groupby('District name')
            os.makedirs(result_loc.joinpath(slug, 'districts'), exist_ok=True)
            for district_name, user_data in user_df.groupby('District name'):
                user_data.to_csv(result_loc.joinpath(slug, 'districts', district_name.lower()+".csv"), index=False)

            shutil.move(result_loc.joinpath(slug, 'validated-user-detail-state.csv'),
                result_loc.joinpath(slug, 'districts', 'validated-user-detail-state.csv'))
            shutil.make_archive(str(result_loc.joinpath(slug, 'districts')),
                                'zip',
                                str(result_loc.joinpath(slug, 'districts')))
            post_data_to_blob(result_loc.joinpath(slug, 'districts.zip'))