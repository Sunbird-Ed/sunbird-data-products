import json
import pdb
import os
import pandas as pd

from datetime import datetime, timedelta, date
from pathlib import Path
from azure.common import AzureMissingResourceHttpError

from dataproducts.util.utils import create_json, get_dqp_data_from_blob, \
    post_data_to_blob, push_metric_event

class DQPMerger:
    def __init__(self, report_config):
        self.report_config = report_config
        self.base_path = None


    def drop_stale(self, report_df_, delta_df_):
        dims = self.report_config['merge']['dims']
        query = []

        try:
            grouped_data = delta_df_[dims].drop_duplicates()
        except Exception as e:
            raise Exception('Dimensions are not available in delta_file')

        for index, delta_row in delta_df_.iterrows():
            formatted_query = ' and '.join(["`{}`!='{}'".format(dim, delta_row[dim]) for dim in dims])
            query.append("({})".format(formatted_query))

        query = " or ".join(query)

        return report_df_.query(query)


    def rollup_report(self, report_df_):
        maxDate = report_df_[self.report_config['rollupCol']].max()

        return report_df_


    def merge_file(self, file_paths):
        print(file_paths['reportPath'])
        report_path = file_paths['reportPath']
        delta_path = file_paths['deltaPath']
        report_path = report_path[1:] if report_path[0] == '/' else report_path
        delta_path = delta_path[1:] if delta_path[0] == '/' else delta_path

        try:
            os.makedirs(self.base_path.joinpath(delta_path).parent, exist_ok=True)
            # get_dqp_data_from_blob(delta_path, self.base_path)
            delta_df = pd.read_csv(self.base_path.joinpath(delta_path))
        except:
            print('INFO::delta file is not available', delta_path)
            return True

        try:
            os.makedirs(self.base_path.joinpath(report_path).parent, exist_ok=True)
            # get_dqp_data_from_blob(report_path, self.base_path)
            report_df = pd.read_csv(self.base_path.joinpath(report_path))
        except:
            report_df = pd.DataFrame()

            for col in delta_df.columns.to_list():
                report_df[col] = report_df.get(col, pd.Series(index=report_df.index, name=col))

        if self.report_config['rollup']:
            report_df = self.drop_stale(report_df, delta_df)
            report_df = pd.concat([report_df, delta_df])

            report_df = self.rollup_report(report_df)
        else:
            report_df = delta_df

        report_df.to_csv(self.base_path.joinpath(report_path), index=False)
        create_json(self.base_path.joinpath(report_path))
        post_data_to_blob(self.base_path.joinpath(report_path))


    def init(self):
        self.base_path = Path(self.report_config['basePath']).joinpath('dqp')
        os.makedirs(self.base_path, exist_ok=True)
        print('START::DQP Merger')
        for file_paths in self.report_config['merge']['files']:
            self.merge_file(file_paths)
        print('SUCCESS::DQP Merger')
