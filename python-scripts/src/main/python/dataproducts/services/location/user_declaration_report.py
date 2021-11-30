"""
Persona wise segragation of User Self-declaration detail report
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

class UserDeclarationReport:
    def __init__(self, data_store_location, states, is_private):
        self.data_store_location = Path(data_store_location)
        self.states = states
        self.is_private = is_private


    def init(self):
        result_loc = self.data_store_location.joinpath('location')
        for slug in self.states.split(","):
            slug = slug.strip()
            state_result_loc = result_loc.joinpath(slug)
            os.makedirs(state_result_loc, exist_ok=True)
            try:
                get_data_from_blob(state_result_loc.joinpath('declared_user_detail', '{}.csv'.format(slug)), is_private=self.is_private)
            except Exception as e:
                print("declared_user_detail not available for "+slug)
                continue
                

            os.makedirs(state_result_loc.joinpath('personas'), exist_ok=True)

            shutil.move(state_result_loc.joinpath('declared_user_detail', '{}.csv'.format(slug)),
                        state_result_loc.joinpath('personas', '{}.csv'.format(slug)))

            shutil.make_archive(str(state_result_loc.joinpath('declared_user_detail', slug)),
                                'zip',
                                str(state_result_loc.joinpath('personas')))
            post_data_to_blob(state_result_loc.joinpath('declared_user_detail', '{}.zip'.format(slug)), is_private=self.is_private)