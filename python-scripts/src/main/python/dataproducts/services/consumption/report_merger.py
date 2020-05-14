import json
import pdb
import os
import pandas as pd

from datetime import datetime, timedelta, date
from pathlib import Path

from dataproducts.util.utils import create_json, download_file_from_store, \
    post_data_to_blob, push_metric_event, get_data_from_blob

class ReportMerger:
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

        for index, delta_row in grouped_data.iterrows():
            formatted_query = ' and '.join(["`{}`!='{}'".format(dim, delta_row[dim]) for dim in dims])
            query.append("({})".format(formatted_query))

        query = " or ".join(query)

        return report_df_.query(query)


    def rollup_report(self, report_df_):
        rollupCol = self.report_config['rollupCol']
        rollupRange = self.report_config['rollupRange'] - 1
        report_df_[rollupCol] = pd.to_datetime(report_df_[rollupCol])

        endDate = report_df_[rollupCol].max().date()
        endYear = endDate.year
        endMonth = endDate.month
        endDay = endDate.day

        if self.report_config['rollupAge'] == 'ACADEMIC_YEAR':
            if endMonth <= 5:
                endYear = endYear - 1 - rollupRange
            else:
                endYear = endYear - rollupRange
            startDate = date(endYear, 6, 1)
        elif self.report_config['rollupAge'] == 'GEN_YEAR':
            endYear = endYear - rollupRange
            startDate = date(endYear, 1, 1)
        elif self.report_config['rollupAge'] == 'MONTH':
            endMonth = endMonth - rollupRange
            endYear = endYear + int(
                        pd.np.floor((endMonth if endMonth != 0 else -1)/12)
                      ) if endMonth < 1 else endYear
            endMonth = endMonth + 12 if endMonth < 1 else endMonth
            startDate = date(endYear, endMonth, 1)
        elif self.report_config['rollupAge'] == 'WEEK':
            startDate = endDate - timedelta(days=endDate.weekday(), weeks=rollupRange)
        elif self.report_config['rollupAge'] == 'DAY':
            startDate = endDate - timedelta(days=rollupRange)

        report_df_ = report_df_[report_df_[rollupCol] >= pd.to_datetime(startDate)]

        report_df_[rollupCol] = report_df_[rollupCol].astype(str)

        return report_df_


    def merge_file(self, file_paths):
        print(file_paths['reportPath'])
        report_path = file_paths['reportPath']
        delta_path = file_paths['deltaPath']
        report_path = report_path[1:] if report_path[0] == '/' else report_path
        delta_path = delta_path[1:] if delta_path[0] == '/' else delta_path

        try:
            os.makedirs(self.base_path.joinpath(delta_path).parent, exist_ok=True)

            download_file_from_store(
                container_name=self.report_config['container'],
                blob_name=delta_path,
                file_path=str(self.base_path.joinpath(delta_path))
                )
            delta_df = pd.read_csv(self.base_path.joinpath(delta_path))
        except Exception as e:
            print('ERROR::delta file is not available', delta_path)
            return True

        try:
            os.makedirs(self.base_path.joinpath(report_path).parent, exist_ok=True)
            get_data_from_blob(self.base_path.joinpath(report_path))
            report_df = pd.read_csv(self.base_path.joinpath(report_path))
        except Exception as e:
            print('INFO::report file is not available', report_path)
            report_df = pd.DataFrame()

            for col in delta_df.columns.to_list():
                report_df[col] = report_df.get(col, pd.Series(index=report_df.index, name=col))

        if self.report_config['rollup']:
            report_df = self.drop_stale(report_df, delta_df)
            report_df = pd.concat([report_df, delta_df])

            report_df = self.rollup_report(report_df)

            report_df = report_df.sort_values(by=self.report_config['rollupCol'], ascending=False)
        else:
            report_df = delta_df

        report_df.to_csv(self.base_path.joinpath(report_path), index=False)
        create_json(self.base_path.joinpath(report_path))
        post_data_to_blob(self.base_path.joinpath(report_path))


    def init(self):
        self.base_path = Path(self.report_config['basePath']).joinpath('report_merger')
        os.makedirs(self.base_path, exist_ok=True)
        print('START::Report Merger')
        for file_paths in self.report_config['merge']['files']:
            self.merge_file(file_paths)
        print('SUCCESS::Report Merger')
