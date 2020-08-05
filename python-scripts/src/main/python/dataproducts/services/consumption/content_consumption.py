"""
content level plays, timespent and ratings by week
"""
import json
import os
import pandas as pd
import pdb
import requests
import shutil
import sys
import time
from azure.common import AzureMissingResourceHttpError
from dataproducts.util.utils import create_json, get_batch_data_from_blob, get_content_plays, get_content_model, \
    get_data_from_blob, get_tenant_info, get_tb_content_mapping, mime_type, post_data_to_blob, push_metric_event
from datetime import datetime, timedelta, date
from pathlib import Path
from string import Template
from pyspark.sql import SparkSession
from pyspark.sql import functions as fn


class ContentConsumption:
    def __init__(self, data_store_location, org_search, druid_hostname, druid_rollup_hostname, content_search,
                 execution_date=date.today().strftime("%d/%m/%Y")):
        self.data_store_location = data_store_location
        self.org_search = org_search
        self.druid_hostname = druid_hostname
        self.druid_rollup_hostname = druid_rollup_hostname
        self.content_search = content_search
        self.execution_date = execution_date
        self.config = {}

    @staticmethod
    def get_overall_report(result_loc_, druid_rollup_, date_, config):
        """
        Query Druid Rollup monthwise for content and platform level play session counts and time_spent.
        :param result_loc_: pathlib.Path() object to store resultant data at
        :param druid_rollup_: Druid broker ip and port for rollup data
        :param date_: execution date for report
        :param config: diksha configurables
        :return: None
        """
        tenant_info = pd.read_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'tenant_info.csv'))[['id', 'slug']]
        tenant_info['id'] = tenant_info['id'].astype(str)
        tenant_info.set_index('id', inplace=True)
        content_model = pd.read_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'content_model_snapshot.csv'))
        content_model['channel'] = content_model['channel'].astype(str)
        content_model['mimeType'] = content_model['mimeType'].apply(mime_type)
        content_model = content_model[
            ['channel', 'board', 'medium', 'gradeLevel', 'subject', 'contentType', 'identifier', 'name', 'creator',
             'mimeType', 'createdOn', 'lastPublishedOn', 'tb_id', 'tb_name', 'me_totalRatings', 'me_averageRating']]
        content_model.set_index('identifier', inplace=True)
        result_loc_.joinpath(date_.strftime('%Y-%m-%d')).mkdir(exist_ok=True)
        start_date = datetime(2019, 6, 1)
        while start_date < date_:
            if datetime(start_date.year + int(start_date.month / 12), (start_date.month % 12) + 1, 1) < date_:
                end_date = datetime(start_date.year + int(start_date.month / 12), (start_date.month % 12) + 1, 1)
            else:
                end_date = date_
            get_content_plays(result_loc_=result_loc_.joinpath(date_.strftime('%Y-%m-%d')), start_date_=start_date,
                              end_date_=end_date, druid_=druid_rollup_, config_=config)
            start_date = end_date
        spark = SparkSession.builder.appName('content_consumption').master("local[*]").getOrCreate()
        content_plays = spark.read.csv(str(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'content_plays_*.csv')),
                                       header=True)
        content_plays = content_plays.groupby(
            fn.col('object_id'),
            fn.col('dimensions_pdata_id')
        ).agg(
            fn.sum('Number of plays').alias('Number of plays'),
            fn.sum('Total time spent').alias('Total time spent')
        ).toPandas()
        spark.stop()
        content_plays = content_plays.pivot(index='object_id', columns='dimensions_pdata_id',
                                            values=['Number of plays', 'Total time spent'])
        col_list = []
        for i in content_plays.columns:
            col_list.append(i[0] + ' on ' + i[1].split('.')[-1].title())
        content_plays.columns = col_list
        content_plays.fillna(0, inplace=True)
        content_plays['Total No of Plays (App and Portal)'] = content_plays['Number of plays on App'] + content_plays[
            'Number of plays on Portal']
        content_plays['Average Play Time in mins on App'] = round(
            content_plays['Total time spent on App'] / (content_plays['Number of plays on App'] * 60), 2)
        content_plays['Average Play Time in mins on Portal'] = round(
            content_plays['Total time spent on Portal'] / (content_plays['Number of plays on Portal'] * 60), 2)
        content_plays['Average Play Time in mins (On App and Portal)'] = round(
            (content_plays['Total time spent on App'] + content_plays['Total time spent on Portal']) / (
                    (content_plays['Number of plays on App'] + content_plays['Number of plays on Portal']) * 60), 2)
        content_plays.drop(['Total time spent on App', 'Total time spent on Portal'], axis=1, inplace=True)
        overall = content_model.join(content_plays).reset_index()
        overall = overall[['channel', 'board', 'medium', 'gradeLevel', 'subject', 'identifier',
                           'name', 'mimeType', 'createdOn', 'creator', 'lastPublishedOn',
                           'tb_id', 'tb_name', 'me_averageRating', 'me_totalRatings',
                           'Number of plays on App', 'Number of plays on Portal',
                           'Total No of Plays (App and Portal)', 'Average Play Time in mins on App',
                           'Average Play Time in mins on Portal',
                           'Average Play Time in mins (On App and Portal)']]
        overall.columns = ['channel', 'Board', 'Medium', 'Grade', 'Subject', 'Content ID', 'Content Name',
                           'Mime Type', 'Created On', 'Creator (User Name)', 'Last Published On',
                           'Linked Textbook Id(s)', 'Linked Textbook Name(s)',
                           'Average Rating(out of 5)', 'Total No of Ratings',
                           'Number of Plays on App', 'Number of Plays on Portal', 'Total No of Plays (App and Portal)',
                           'Average Play Time in mins on App', 'Average Play Time in mins on Portal',
                           'Average Play Time in mins (On App and Portal)']
        overall['Content ID'] = overall['Content ID'].str.replace('.img', '')
        overall['Created On'] = overall['Created On'].fillna('T').apply(
            lambda x: '-'.join(x.split('T')[0].split('-')[::-1]))
        overall['Last Published On'] = overall['Last Published On'].fillna('T').apply(
            lambda x: '-'.join(x.split('T')[0].split('-')[::-1]))
        overall.fillna({
            'Board': 'Unknown',
            'Medium': 'Unknown',
            'Grade': 'Unknown',
            'Subject': 'Unknown',
            'Creator (User Name)': '',
            'Linked Textbook Id(s)': '',
            'Linked Textbook Name(s)': '',
            'Number of Plays on App': 0,
            'Number of Plays on Portal': 0,
            'Total No of Plays (App and Portal)': 0,
            'Average Play Time in mins on App': 0,
            'Average Play Time in mins on Portal': 0,
            'Average Play Time in mins (On App and Portal)': 0
        }, inplace=True)
        overall.sort_values(inplace=True, ascending=[1, 1, 1, 1, 1, 0],
                            by=['channel', 'Board', 'Medium', 'Grade', 'Subject', 'Total No of Plays (App and Portal)'])
        for channel in overall.channel.unique():
            try:
                slug = tenant_info.loc[channel]['slug']
            except KeyError:
                continue
            content_aggregates = overall[overall['channel'] == channel]
            content_aggregates.drop(['channel'], axis=1, inplace=True)
            result_loc_.parent.joinpath('portal_dashboards', slug).mkdir(exist_ok=True)
            content_aggregates.to_csv(result_loc_.parent.joinpath('portal_dashboards', slug, 'content_aggregated.csv'),
                                      index=False, encoding='utf-8-sig')
            create_json(result_loc_.parent.joinpath('portal_dashboards', slug, 'content_aggregated.csv'))
            post_data_to_blob(result_loc_.parent.joinpath('portal_dashboards', slug, 'content_aggregated.csv'))

    @staticmethod
    def get_weekly_report(result_loc_, druid_rollup_, date_, config):
        """
        Query druid rollups for weekly content and platform level play session counts and time_spent.
        :param result_loc_: pathlib.Path() object to store resultant CSVs at
        :param druid_rollup_: druid broker ip and port for rollup data
        :param date_: execution date for report
        :param config: diksha configurables
        :return: None
        """
        tenant_info = pd.read_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'tenant_info.csv'))[['id', 'slug']]
        tenant_info['id'] = tenant_info['id'].astype(str)
        tenant_info.set_index('id', inplace=True)
        content_model = pd.read_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'content_model_snapshot.csv'))
        content_model['channel'] = content_model['channel'].astype(str)
        content_model['mimeType'] = content_model['mimeType'].apply(mime_type)
        content_model = content_model[
            ['channel', 'board', 'medium', 'gradeLevel', 'subject', 'contentType', 'identifier', 'name', 'creator',
             'mimeType', 'createdOn', 'lastPublishedOn', 'tb_id', 'tb_name', 'me_totalRatings', 'me_averageRating']]
        content_model.set_index('identifier', inplace=True)
        result_loc_.joinpath(date_.strftime('%Y-%m-%d')).mkdir(exist_ok=True)
        start_date = date_ - timedelta(days=7)
        get_content_plays(result_loc_=result_loc_.joinpath(date_.strftime('%Y-%m-%d')), start_date_=start_date,
                          end_date_=date_, druid_=druid_rollup_, config_=config)
        content_plays = pd.read_csv(
            result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'content_plays_{}.csv'.format(date_.strftime('%Y-%m-%d'))))
        content_plays = content_plays.groupby(['object_id', 'dimensions_pdata_id'])[
            ['Number of plays', 'Total time spent']].sum().reset_index()
        content_plays = content_plays.pivot(index='object_id', columns='dimensions_pdata_id',
                                            values=['Number of plays', 'Total time spent'])
        col_list = []
        for i in content_plays.columns:
            col_list.append(i[0] + ' on ' + i[1].split('.')[-1].title())
        content_plays.columns = col_list
        content_plays.fillna(0, inplace=True)
        content_plays['Total No of Plays (App and Portal)'] = content_plays['Number of plays on App'] + content_plays[
            'Number of plays on Portal']
        content_plays['Average Play Time in mins on App'] = round(
            content_plays['Total time spent on App'] / (content_plays['Number of plays on App'] * 60), 2)
        content_plays['Average Play Time in mins on Portal'] = round(
            content_plays['Total time spent on Portal'] / (content_plays['Number of plays on Portal'] * 60), 2)
        content_plays['Average Play Time in mins (On App and Portal)'] = round(
            (content_plays['Total time spent on App'] + content_plays['Total time spent on Portal']) / (
                    (content_plays['Number of plays on App'] + content_plays['Number of plays on Portal']) * 60), 2)
        content_plays.drop(['Total time spent on App', 'Total time spent on Portal'], axis=1, inplace=True)
        weekly = content_model.join(content_plays).reset_index()
        weekly = weekly[['channel', 'board', 'medium', 'gradeLevel', 'subject', 'identifier',
                         'name', 'mimeType', 'createdOn', 'creator', 'lastPublishedOn',
                         'tb_id', 'tb_name', 'me_averageRating', 'me_totalRatings',
                         'Number of plays on App', 'Number of plays on Portal',
                         'Total No of Plays (App and Portal)', 'Average Play Time in mins on App',
                         'Average Play Time in mins on Portal',
                         'Average Play Time in mins (On App and Portal)']]
        weekly.columns = ['channel', 'Board', 'Medium', 'Grade', 'Subject', 'Content ID', 'Content Name',
                          'Mime Type', 'Created On', 'Creator (User Name)', 'Last Published On',
                          'Linked Textbook Id(s)', 'Linked Textbook Name(s)',
                          'Average Rating(out of 5)', 'Total No of Ratings',
                          'Number of Plays on App', 'Number of Plays on Portal', 'Total No of Plays (App and Portal)',
                          'Average Play Time in mins on App', 'Average Play Time in mins on Portal',
                          'Average Play Time in mins (On App and Portal)']
        weekly['Content ID'] = weekly['Content ID'].str.replace('.img', '')
        weekly['Created On'] = weekly['Created On'].fillna('T').apply(
            lambda x: '-'.join(x.split('T')[0].split('-')[::-1]))
        weekly['Last Published On'] = weekly['Last Published On'].fillna('T').apply(
            lambda x: '-'.join(x.split('T')[0].split('-')[::-1]))
        weekly.fillna({
            'Board': 'Unknown',
            'Medium': 'Unknown',
            'Grade': 'Unknown',
            'Subject': 'Unknown',
            'Creator (User Name)': '',
            'Linked Textbook Id(s)': '',
            'Linked Textbook Name(s)': '',
            'Number of Plays on App': 0,
            'Number of Plays on Portal': 0,
            'Total No of Plays (App and Portal)': 0,
            'Average Play Time in mins on App': 0,
            'Average Play Time in mins on Portal': 0,
            'Average Play Time in mins (On App and Portal)': 0
        }, inplace=True)
        weekly.sort_values(inplace=True, ascending=[1, 1, 1, 1, 1, 0],
                           by=['channel', 'Board', 'Medium', 'Grade', 'Subject', 'Total No of Plays (App and Portal)'])
        weekly['Last Date of the week'] = date_.strftime('%d-%m-%Y')
        for channel in weekly.channel.unique():
            try:
                slug = tenant_info.loc[channel]['slug']
            except KeyError:
                continue
            content_aggregates = weekly[weekly['channel'] == channel]
            content_aggregates.drop(['channel'], axis=1, inplace=True)
            result_loc_.parent.joinpath('portal_dashboards', slug).mkdir(exist_ok=True)
            content_aggregates.to_csv(
                result_loc_.parent.joinpath('portal_dashboards', slug, 'content_consumption_lastweek.csv'),
                index=False, encoding='utf-8-sig')
            create_json(result_loc_.parent.joinpath('portal_dashboards', slug, 'content_consumption_lastweek.csv'))
            post_data_to_blob(
                result_loc_.parent.joinpath('portal_dashboards', slug, 'content_consumption_lastweek.csv'))
            content_aggregates.to_csv(
                result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'content_consumption_lastweek_{}.csv'.format(slug)),
                index=False, encoding='utf-8-sig')
            post_data_to_blob(
                result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'content_consumption_lastweek_{}.csv'.format(slug)),
                backup=True)

    @staticmethod
    def get_last_week_report(result_loc_, date_, num_weeks):
        """
        fetch last n weekly reports from storage and zip folder
        :param result_loc_: pathlib.Path() object to store resultant csv at
        :param date_: execution date
        :param num_weeks: number of weeks to fetch report for
        :return: None
        """
        result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'content_consumption').mkdir(exist_ok=True)
        for i in range(num_weeks):
            last_week = date_ - timedelta(days=((i + 1) * 7))
            result_loc_.joinpath(last_week.strftime('%Y-%m-%d')).mkdir(exist_ok=True)
            get_batch_data_from_blob(
                result_loc_=result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'content_consumption'),
                prefix_=result_loc_.name + '/' + last_week.strftime('%Y-%m-%d'),
                backup=True
            )
        for slug in result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'content_consumption').iterdir():
            if slug.is_dir():
                shutil.make_archive(
                    str(result_loc_.parent.joinpath('portal_dashboards', slug.name, 'content_consumption')), 'zip',
                    str(slug))
                post_data_to_blob(
                    result_loc_.parent.joinpath('portal_dashboards', slug.name, 'content_consumption.zip'))

    def init(self):
        start_time_sec = int(round(time.time()))
        print("Content Consumption Report::Start")
        self.data_store_location = Path(self.data_store_location)
        org_search = self.org_search
        druid = self.druid_hostname
        druid_rollup = self.druid_rollup_hostname
        content_search = self.content_search
        execution_date = datetime.strptime(self.execution_date, "%d/%m/%Y")
        result_loc = self.data_store_location.joinpath('content_plays')
        result_loc.parent.joinpath('portal_dashboards').mkdir(exist_ok=True)
        result_loc.parent.joinpath('config').mkdir(exist_ok=True)
        get_data_from_blob(result_loc.parent.joinpath('config', 'diksha_config.json'))
        with open(result_loc.parent.joinpath('config', 'diksha_config.json'), 'r') as f:
            self.config = json.loads(f.read())
        get_tenant_info(result_loc_=result_loc, org_search_=org_search, date_=execution_date)
        print("Success::Tenant info")
        get_content_model(result_loc_=result_loc, druid_=druid, date_=execution_date, config_=self.config)
        print("Success::content model snapshot")
        get_tb_content_mapping(result_loc_=result_loc, date_=execution_date, content_search_=content_search)
        print("Success::TB Content Map")
        self.get_weekly_report(result_loc_=result_loc, druid_rollup_=druid_rollup, date_=execution_date,
                               config=self.config)
        print("Success::Weekly Conent Consumption")
        self.get_overall_report(result_loc_=result_loc, druid_rollup_=druid_rollup, date_=execution_date,
                                config=self.config)
        print("Success::Overall Conent Consumption")
        self.get_last_week_report(result_loc_=result_loc, date_=execution_date, num_weeks=6)
        print("Success:::Last 6 weeks")
        shutil.rmtree(result_loc)
        print("Content Consumption Report::Completed")
        end_time_sec = int(round(time.time()))
        time_taken = end_time_sec - start_time_sec
        metrics = [
            {
                "metric": "timeTakenSecs",
                "value": time_taken
            },
            {
                "metric": "date",
                "value": execution_date.strftime("%Y-%m-%d")
            }
        ]
        push_metric_event(metrics, "Content Consumption Metrics")
