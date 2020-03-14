"""
Generate a report that specifies number of contents created in last week and overall across Live, Review and Draft.
"""
import re
import sys, time
import os
import pdb
import requests
import pandas as pd

from datetime import date, timedelta, datetime
from pathlib import Path
from string import Template
from time import sleep

from dataproducts.util.utils import get_tenant_info, create_json, \
                post_data_to_blob, get_content_model, push_metric_event

class ContentCreationStatus:
    def __init__(self, data_store_location, org_search, druid_hostname,
                 execution_date=date.today().strftime("%d/%m/%Y")):
        self.data_store_location = Path(data_store_location)
        self.org_search = org_search
        self.druid_hostname = druid_hostname
        self.execution_date = execution_date


    def date_format(self, date_):
        return date_.split('T')[0]


    def grade_sort(self, series):
        try:
            result = None
            value = sorted(list(map(int, re.findall('\d+', series))))
            other = re.findall('Other', series)
            if value and other:
                result = value[0]
                if len(value) > 1:
                    if value[-1] < 10:
                        result += value[-1] / 10.0
                    else:
                        temp = 0.9
                        result += temp + value[-1] / 1000.0
                result += 0.003
            elif value and not other:
                result = value[0]
                if len(value) > 1:
                    if value[-1] < 10:
                        result += value[-1] / 10.0
                    else:
                        temp = 0.9
                        result += temp + value[-1] / 1000.0
            elif not value and other:
                result = 13
            elif not value and not other:
                result = 14
        except TypeError:
            result = 14
        return result


    def generate_report(self, result_loc_, date_):
        board_slug = pd.read_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'tenant_info.csv'))[[
            'id', 'slug'
        ]]

        df = pd.read_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'content_model_snapshot.csv'))[[
            'board','medium','gradeLevel','subject','identifier','name','status',
            'createdOn','creator','lastPublishedOn','lastUpdatedOn', 'channel'
        ]]


        if 'createdOn' not in df.columns:
            df['createdOn'] = 'T'
        if 'lastSubmittedOn' not in df.columns:
            df['lastSubmittedOn'] = 'T'
        if 'lastPublishedOn' not in df.columns:
            df['lastPublishedOn'] = 'T'

        df['createdOn'] = df['createdOn'].fillna('T').apply(self.date_format)

        review = df[df['status'] == 'Review']
        review['lastSubmittedOn'] = review['lastSubmittedOn'].fillna('T').apply(self.date_format)
        review.rename(columns={'lastSubmittedOn': 'Pending in current status since'}, inplace=True)

        only_draft = df[(df['status'] == 'Draft') | (df['lastPublishedOn'].isna())]
        only_draft.loc[:, 'Pending in current status since'] = only_draft.loc[:, 'createdOn']

        published = df[(df['status'] == 'Unlisted') | \
                       (df['status'] == 'Live') | \
                       ((df['status'] == 'Draft') & (df['lastPublishedOn'].notna()))]
        published['status'] = pd.np.where(published['status'] == 'Unlisted', 'Limited Sharing', published['status'])
        published['lastPublishedOn'] = published['lastPublishedOn'].fillna('T').apply(self.date_format)
        published.rename(columns={'lastPublishedOn': 'Pending in current status since'}, inplace=True)

        result_df = pd.concat([review, only_draft, published])
        result_df['gradeSort'] = result_df['gradeLevel'].apply(self.grade_sort)

        result_df = result_df.sort_values(
            by=['board', 'medium', 'gradeSort', 'subject', 'name'],
            ascending=[False, True, True, True, True])

        result_df = result_df[[
            'board','medium','gradeLevel','subject','identifier','name','status',
            'createdOn', 'Pending in current status since', 'creator', 'channel'
        ]]

        result_df.to_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'Content_Creation_Status_Overall.csv'),
            index=False, encoding='utf-8')
        # create_json(result_loc_.joinpath('Content_Creation_Status.csv'))
        post_data_to_blob(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'Content_Creation_Status_Overall.csv'), True)

        for index, bs_value in board_slug.iterrows():
            channel, slug = bs_value.values
            print(slug)

            channel_df = result_df[result_df['channel'] == channel]
            channel_df = channel_df[['board','medium','gradeLevel','subject','identifier','name','status',
                                     'createdOn', 'Pending in current status since', 'creator']]
            channel_df.columns = ['Board','Medium','Grade','Subject','Content Id','Content name',
                                  'Status', 'Created On', 'Pending in current status since', 'Created By']

            os.makedirs(result_loc_.joinpath(slug), exist_ok=True)
            channel_df.to_csv(result_loc_.joinpath(slug, 'Content_Creation_Status.csv'), index=False, encoding='utf-8')
            create_json(result_loc_.joinpath(slug, 'Content_Creation_Status.csv'))
            post_data_to_blob(result_loc_.joinpath(slug, 'Content_Creation_Status.csv'))


    def init(self):
        start_time_sec = int(round(time.time()))
        print('Content Creation Status Report::Start')
        result_loc = self.data_store_location.joinpath('content_creation')
        result_loc.mkdir(exist_ok=True)

        execution_date = datetime.strptime(self.execution_date, "%d/%m/%Y")
        # get_tenant_info(result_loc_=result_loc, org_search_=self.org_search, date_=execution_date)

        # get_content_model(result_loc_=result_loc, druid_=self.druid_hostname,
        #             date_=execution_date, status_=['Live', 'Review', 'Draft', 'Unlisted'])

        self.generate_report(result_loc, execution_date)
        print('Content Creation Status Report::Completed')
        end_time_sec = int(round(time.time()))
        time_taken = end_time_sec - start_time_sec
        metrics = [
            {
                'metric': 'timeTakenSecs',
                'value': time_taken
            },
            {
                'metric': 'date',
                'value': execution_date.strftime('%Y-%m-%d')
            }
        ]
        push_metric_event(metrics, 'Content Creation Status')