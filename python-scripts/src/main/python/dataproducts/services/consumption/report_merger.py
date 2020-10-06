import json
import pdb
import os
import pandas as pd

from copy import deepcopy
from datetime import datetime, timedelta, date
from pathlib import Path

from dataproducts.util.utils import create_json, download_file_from_store, \
    upload_file_to_store, post_data_to_blob, push_metric_event, get_data_from_blob

class ReportMerger:
    def __init__(self, report_config):
        self.report_config = report_config
        self.base_path = None

        if self.report_config['rollup']:
            rollup_splitted = self.report_config['rollupCol'].split('||')
            self.rollupCol = rollup_splitted[0]
            self.rollupColFormat = rollup_splitted[1] if len(rollup_splitted) > 1 else None


    def drop_stale(self, report_df_, delta_df_):
        if '||' in self.report_config['rollupCol']:
            dims = [self.rollupCol]
        else:
            dims = self.report_config['merge']['dims']

        query = []

        try:
            grouped_data = delta_df_[dims].drop_duplicates()
        except Exception as e:
            raise Exception('Dimensions are not available in delta_file')

        column_history = {}
        for i, dim in enumerate(deepcopy(dims)):
            dim_init = deepcopy(dim)
            dim = ''.join(e for e in dim if e.isalnum())
            dims[i] = dim
            column_history[dim] = dim_init
            grouped_data.rename(columns={dim_init: dim}, inplace=True)
            report_df_.rename(columns={dim_init: dim}, inplace=True)

        for index, delta_row in grouped_data.iterrows():
            formatted_query = ' and '.join(["`{}`!='{}'".format(dim, delta_row[dim]) for dim in dims])
            query.append("({})".format(formatted_query))

        query = " or ".join(query)
        report_df_ = report_df_.query(query)

        return report_df_.rename(columns=column_history)


    def rollup_report(self, report_df_):
        rollupRange = self.report_config['rollupRange'] - 1

        endDate = report_df_[self.rollupCol].max().date()
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

        report_df_ = report_df_[report_df_[self.rollupCol] >= pd.to_datetime(startDate)]

        return report_df_


    def merge_file(self, file_paths):
        print(file_paths['reportPath'])
        report_path = file_paths['reportPath']
        delta_path = file_paths['deltaPath']
        report_path = report_path[1:] if report_path[0] == '/' else report_path
        delta_path = delta_path[1:] if delta_path[0] == '/' else delta_path
        file_path = self.base_path.joinpath(report_path)
        col_order = []
        report_container = self.report_config.get('postContainer') if self.report_config.get('postContainer') else 'report-verification'
        delta_file_access = self.report_config.get('deltaFileAccess') if 'deltaFileAccess' in self.report_config else True
        report_file_access = self.report_config.get('reportFileAccess') if 'reportFileAccess' in self.report_config else True

        try:
            os.makedirs(self.base_path.joinpath(delta_path).parent, exist_ok=True)

            download_file_from_store(
                container_name=self.report_config['container'],
                blob_name=delta_path,
                file_path=str(self.base_path.joinpath(delta_path)),
                is_private=delta_file_access
                )
            delta_df = pd.read_csv(self.base_path.joinpath(delta_path))
            col_order = delta_df.columns.to_list()
            if self.report_config['rollup']:
                if 'Date' in col_order:
                    delta_df.rename(columns={'Date': self.rollupCol}, inplace=True)
        except Exception as e:
            print('ERROR::delta file is not available', delta_path)
            return True

        try:
            os.makedirs(file_path.parent, exist_ok=True)
            download_file_from_store(
                container_name=report_container,
                blob_name=report_path,
                file_path=str(file_path),
                is_private=report_file_access
            )
            report_df = pd.read_csv(file_path)
            col_order = report_df.columns.to_list()
        except Exception as e:
            print('INFO::report file is not available', report_path)
            report_df = pd.DataFrame()

            for col in col_order:
                report_df[col] = report_df.get(col, pd.Series(index=report_df.index, name=col))

        if self.report_config['rollup']:
            if self.rollupCol in report_df.columns.to_list():
                report_df[self.rollupCol] = pd.to_datetime(report_df[self.rollupCol], format=self.rollupColFormat)

            delta_df[self.rollupCol] = pd.to_datetime(delta_df[self.rollupCol], format="%Y-%m-%d")

            report_df = self.drop_stale(report_df, delta_df)
            report_df = pd.concat([report_df, delta_df])
            report_df = self.rollup_report(report_df)

            report_df = report_df.sort_values(by=self.rollupCol)

            report_df[self.rollupCol] = report_df[self.rollupCol].dt.strftime(self.rollupColFormat)
        else:
            report_df = delta_df

        report_df = report_df.reindex(columns=col_order)
        report_df.to_csv(file_path, index=False)
        create_json(file_path)
        upload_file_to_store(
                container_name=report_container,
                blob_name=file_path.parent.name + '/' + file_path.name,
                file_path=str(file_path),
                is_private=report_file_access
                )
        upload_file_to_store(
                container_name=report_container,
                blob_name=file_path.parent.name + '/' + file_path.name.replace('.csv', '.json'),
                file_path=str(file_path).replace('.csv', '.json'),
                is_private=report_file_access
                )


    def init(self):
        self.base_path = Path(self.report_config['basePath']).joinpath('report_merger')
        os.makedirs(self.base_path, exist_ok=True)
        print('START::Report Merger')
        for file_paths in self.report_config['merge']['files']:
            self.merge_file(file_paths)
        print('SUCCESS::Report Merger')
