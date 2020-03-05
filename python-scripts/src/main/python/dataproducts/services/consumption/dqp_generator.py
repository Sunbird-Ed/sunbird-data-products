import json
import pandas as pd

from datetime import datetime, timedelta, date
from pathlib import Path
from azure.common import AzureMissingResourceHttpError

from dataproducts.util.utils import create_json, get_tenant_info, get_data_from_blob, \
    post_data_to_blob, get_content_model, get_content_plays, push_metric_event

class DQPGenerator:
    def __init__(self, report_id, data_store_location, main_file_path, delta_file_path,
                 merge_dimensions, merge_keep, merge_mode,
                 granularity, rolling_file_age, rolling_file_range):
        self.report_id = report_id
        self.data_store_location = Path(data_store_location)
        self.main_file_path = main_file_path
        self.delta_file_path = delta_file_path
        self.merge_dimensions = merge_dimensions
        self.merge_keep = merge_keep
        self.merge_mode = merge_mode
        self.granularity = granularity
        self.rolling_file_age = rolling_file_age
        self.rolling_file_range = rolling_file_range


    def init(self):
         get_data_from_blob()