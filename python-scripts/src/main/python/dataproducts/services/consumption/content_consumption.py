"""
content level plays, timespent and ratings by week
"""
import json
import sys, time
import pdb
import os
import requests
import pandas as pd

from datetime import datetime, timedelta, date
from pathlib import Path
from string import Template
from azure.common import AzureMissingResourceHttpError
from cassandra.cluster import Cluster

from dataproducts.util.utils import create_json, get_tenant_info, get_data_from_blob, \
    post_data_to_blob, get_content_model, get_content_plays, push_metric_event

class ContentConsumption:
    def __init__(self, data_store_location, org_search, druid_hostname,
                cassandra_host, keyspace_prefix, content_search,
                execution_date=date.today().strftime("%d/%m/%Y")):
        self.data_store_location = data_store_location
        self.org_search = org_search
        self.druid_hostname = druid_hostname
        self.cassandra_host = cassandra_host
        self.keyspace_prefix = keyspace_prefix
        self.content_search = content_search
        self.execution_date = execution_date
        self.config = {}

    def mime_type(self, series):
        """
        map the content format into preset buckets
        :param series: pandas series
        :return: pandas series
        """
        if series == 'video/x-youtube':
            return 'YouTube Content'
        elif series == 'application/vnd.ekstep.ecml-archive':
            return 'Created on Diksha'
        elif series == 'video/mp4' or series == 'video/webm':
            return 'Uploaded Videos'
        elif series == 'application/pdf' or series == 'application/epub':
            return 'Text Content'
        elif series == 'application/vnd.ekstep.html-archive' or series == 'application/vnd.ekstep.h5p-archive':
            return 'Uploaded Interactive Content'
        else:
            return None


    def define_keyspace(self, cassandra_, keyspace_, replication_factor_=1):
        """
        given cassandra cluster, keyspace and replication factor, ensure the keyspace and table exist
        :param cassandra_: IP address of the Casssandra cluster
        :param keyspace_: Keyspace name for the cassandra cluster
        :param replication_factor_: replication factor used in cassandra cluster
        :return: None
        """
        cluster = Cluster([cassandra_])
        session = cluster.connect()
        keyspace_query = Template("""CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication = {
          'class': 'SimpleStrategy',
          'replication_factor': '$replication_factor'
        }""")
        session.execute(keyspace_query.substitute(keyspace=keyspace_, replication_factor=replication_factor_))
        table_query = Template("""CREATE TABLE IF NOT EXISTS $keyspace.content_aggregates (
            content_id text,
            period int,
            pdata_id text,
            metric map<text, double>,
            PRIMARY KEY (content_id, period, pdata_id)
        )""")
        session.execute(table_query.substitute(keyspace=keyspace_))


    def insert_data_to_cassandra(self, result_loc_, date_, cassandra_, keyspace_):
        """
        Insert the content plays and timespent data into cassandra with primary key on content id, date and pdata_id
        :param result_loc_: local path to store resultant csv
        :param date_: datetime object to pass to file path
        :param cassandra_: ip of the cassandra cluster
        :param keyspace_: keyspace in which we are working
        :return: None
        """
        data = pd.read_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'content_plays.csv'))
        cluster = Cluster([cassandra_])
        session = cluster.connect()
        insert_query = Template("""INSERT INTO $keyspace.content_aggregates(content_id, period, pdata_id, metric) 
        VALUES ('$content_id', $period, '$pdata_id', {'plays': $plays, 'timespent': $timespent})""")
        for ind, row in data.iterrows():
            content_id = row['object_id']
            period = row['Date']
            pdata_id = row['dimensions_pdata_id']
            plays = row['Number of plays']
            timespent = row['Total time spent']
            session.execute(
                insert_query.substitute(keyspace=keyspace_, content_id=content_id, period=period, pdata_id=pdata_id,
                                        plays=plays, timespent=timespent))
        session.shutdown()
        cluster.shutdown()


    def append_tb_mapping(self, result_loc_, df_):
        textbooks = pd.read_csv(result_loc_.joinpath('tb_content_mapping.csv')) \
                      .set_index("identifier")
        df_.set_index("identifier", inplace=True)
        merged_df = df_.join(textbooks, how="left", on="identifier")
        merged_df["tb_id"] = merged_df["tb_id"].fillna("")
        merged_df["tb_name"] = merged_df["tb_name"].fillna("")
        return merged_df.reset_index()


    def get_tb_content_mapping(self, result_loc_, content_search_):
        """
         get a list of textbook from LP API and iterate over the textbook hierarchy to create CSV
        :param result_loc_: pathlib.Path object to store resultant CSV at
        :param content_search_: ip and port of the server hosting LP content search API
        """
        tb_url = "{}v3/search".format(content_search_)
        payload = """{
                    "request": {
                        "filters": {
                            "contentType": ["Textbook"],
                            "status": ["Live"]
                        },
                        "fields": ["childNodes", "identifier", "name"],
                        "sort_by": {"createdOn":"desc"},
                        "limit": 10000
                    }
                }"""
        tb_headers = {
            'content-type': "application/json; charset=utf-8",
            'cache-control': "no-cache"
        }
        retry_count = 0
        while retry_count < 5:
            retry_count += 1
            try:
                response = requests.request("POST", tb_url, data=payload, headers=tb_headers)
                textbooks = pd.DataFrame(response.json()['result']['content'])[
                ["childNodes", "identifier", "name"]]
                textbooks.columns = ["identifier", "tb_id", "tb_name"]
                textbooks = textbooks.explode('identifier')
                textbooks.to_csv(result_loc_.joinpath('tb_content_mapping.csv'), index=False)
                break
            except requests.exceptions.ConnectionError:
                print("Retry {} for textbook list".format(retry_count))
                sleep(10)
        else:
            print("Max retries reached...")


    def get_overall_report(self, result_loc_, date_):
        tenant_info = pd.read_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'tenant_info.csv'))[['id', 'slug']]
        tenant_info['id'] = tenant_info['id'].astype(str)
        tenant_info.set_index('id', inplace=True)
        df = pd.read_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'content_model_snapshot.csv'))
        df = self.append_tb_mapping(result_loc_, df)
        df['creator'] = df['creator'].str.replace('null', '')
        df['channel'] = df['channel'].astype(str)
        df['mimeType'] = df['mimeType'].apply(self.mime_type)

        df['me_totalDownloads'] = df['me_totalDownloads'].fillna(0)
        df['me_totalTimeSpentInApp'] = df['me_totalTimeSpentInApp'].fillna(0)
        df['me_totalTimeSpentInPortal'] = df['me_totalTimeSpentInPortal'].fillna(0)
        df['me_totalPlaySessionCountInApp'] = df['me_totalPlaySessionCountInApp'].fillna(0)
        df['me_totalPlaySessionCountInPortal'] = df['me_totalPlaySessionCountInPortal'].fillna(0)

        df['Total No of Plays (App and Portal)'] = df['me_totalPlaySessionCountInApp'] + \
                                                   df['me_totalPlaySessionCountInPortal']

        df['Average Play Time in mins on App'] = round(
            df['me_totalTimeSpentInApp'] / (60 * df['me_totalPlaySessionCountInApp']), 2)
        df['Average Play Time in mins on Portal'] = round(
            df['me_totalTimeSpentInPortal'] / (60 * df['me_totalPlaySessionCountInPortal']), 2)
        df['Average Play Time in mins (On App and Portal)'] = round(
            (df['Average Play Time in mins on App'] + df['Average Play Time in mins on Portal']) / \
            df['Total No of Plays (App and Portal)'], 2)
        df['Average Play Time in mins on App'] = df['Average Play Time in mins on App'].fillna(0)
        df['Average Play Time in mins on Portal'] = df['Average Play Time in mins on Portal'].fillna(0)
        df['Average Play Time in mins (On App and Portal)'] = df['Average Play Time in mins (On App and Portal)'].fillna(0)

        df = df[['channel', 'board', 'medium', 'gradeLevel', 'subject', 'identifier',
             'name', 'mimeType', 'createdOn', 'creator','lastPublishedOn',
             'tb_id', 'tb_name', 'me_averageRating', 'me_totalRatings',
             'me_totalDownloads', 'me_totalPlaySessionCountInApp', 'me_totalPlaySessionCountInPortal',
             'Total No of Plays (App and Portal)', 'Average Play Time in mins on App', 'Average Play Time in mins on Portal',
             'Average Play Time in mins (On App and Portal)']]

        df.columns = ['channel', 'Board', 'Medium', 'Grade', 'Subject', 'Content ID', 'Content Name',
                     'Mime Type', 'Created On', 'Creator (User Name)', 'Last Published On',
                     'Linked Textbook Id', 'Linked Textbook Name',
                     'Average Rating(out of 5)', 'Total No of Ratings', 'No of Downloads',
                     'Number of Plays on App', 'Number of Plays on Portal', 'Total No of Plays (App and Portal)',
                     'Average Play Time in mins on App','Average Play Time in mins on Portal',
                     'Average Play Time in mins (On App and Portal)']
        df['Content ID'] = df['Content ID'].str.replace('.img', '')
        df['Created On'] = df['Created On'].fillna('T').apply(
            lambda x: '-'.join(x.split('T')[0].split('-')[::-1]))
        df['Last Published On'] = df['Last Published On'].fillna('T').apply(
            lambda x: '-'.join(x.split('T')[0].split('-')[::-1]))

        df = df.fillna('Unknown')
        df.sort_values(inplace=True, ascending=[1, 1, 1, 1, 1, 0],
                       by=['channel', 'Board', 'Medium', 'Grade', 'Subject', 'Total No of Plays (App and Portal)'])
        for channel in df.channel.unique():
            try:
                slug = tenant_info.loc[channel]['slug']
                print(slug)
            except KeyError:
                continue
            content_aggregates = df[df['channel'] == channel]
            content_aggregates.drop(['channel'], axis=1, inplace=True)
            result_loc_.parent.joinpath('portal_dashboards', slug).mkdir(exist_ok=True)

            content_aggregates.to_csv(result_loc_.parent.joinpath('portal_dashboards', slug, 'overall_content_aggregates.csv'),
                                      index=False, encoding='utf-8-sig')
            create_json(result_loc_.parent.joinpath('portal_dashboards', slug, 'overall_content_aggregates.csv'))
            post_data_to_blob(result_loc_.parent.joinpath('portal_dashboards', slug, 'overall_content_aggregates.csv'))

    def get_weekly_plays(self, result_loc_, date_, cassandra_, keyspace_):
        """
        query cassandra table for 1 week of content play and timespent.
        :param result_loc_: local path to store resultant csv
        :param date_: datetime object to pass to file path
        :param cassandra_: ip of the cassandra cluster
        :param keyspace_: keyspace in which we are working
        :return: None
        """
        tenant_info = pd.read_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'tenant_info.csv'))[['id', 'slug']]
        tenant_info['id'] = tenant_info['id'].astype(str)
        tenant_info.set_index('id', inplace=True)
        cluster = Cluster([cassandra_])
        session = cluster.connect()
        start_date = date_ - timedelta(days=7)
        fetch_query = Template("""
        SELECT content_id, period, pdata_id, metric FROM $keyspace.content_aggregates WHERE 
        period >= $start_date AND 
        period < $end_date
        ALLOW FILTERING
        """)
        result = session.execute(fetch_query.substitute(keyspace=keyspace_, start_date=start_date.strftime('%Y%m%d'),
                                                        end_date=date_.strftime('%Y%m%d')))
        df_dict = {}
        for row in result:
            if row.content_id in df_dict.keys():
                pass
            else:
                df_dict[row.content_id] = {
                    'identifier': row.content_id,
                    'Number of Plays on App': 0,
                    'Number of Plays on Portal': 0,
                    'Timespent on App': 0,
                    'Timespent on Portal': 0
                }
            pdata_id = 'App' if row.pdata_id == self.config['context']['pdata']['id']['app'] else 'Portal' if \
                row.pdata_id == self.config['context']['pdata']['id']['portal'] else 'error'
            df_dict[row.content_id]['Number of Plays on ' + pdata_id] += row.metric['plays']
            df_dict[row.content_id]['Timespent on ' + pdata_id] = row.metric['timespent']
        temp = []
        for k, v in df_dict.items():
            temp.append(v)
        df = pd.DataFrame(temp)
        df['Total No of Plays (App and Portal)'] = df['Number of Plays on App'] + df['Number of Plays on Portal']
        df['Average Play Time in mins on App'] = round(df['Timespent on App'] / (60 * df['Number of Plays on App']), 2)
        df['Average Play Time in mins on Portal'] = round(
            df['Timespent on Portal'] / (60 * df['Number of Plays on Portal']), 2)
        df['Average Play Time in mins (On App and Portal)'] = round(
            (df['Timespent on App'] + df['Timespent on Portal']) / (60 * df['Total No of Plays (App and Portal)']), 2)
        df = df[['identifier', 'Total No of Plays (App and Portal)', 'Number of Plays on App', 'Number of Plays on Portal',
                 'Average Play Time in mins (On App and Portal)',
                 'Average Play Time in mins on App',
                 'Average Play Time in mins on Portal']]
        content_model = pd.read_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'content_model_snapshot.csv'))[
            ['channel', 'board', 'medium', 'gradeLevel', 'subject', 'identifier', 'name', 'mimeType', 'createdOn', 'creator',
             'lastPublishedOn', 'me_averageRating']]
        content_model["creator"] = content_model["creator"].str.replace("null", "")
        content_model['channel'] = content_model['channel'].astype(str)
        content_model['mimeType'] = content_model['mimeType'].apply(self.mime_type)
        content_model.columns = ['channel', 'Board', 'Medium', 'Grade', 'Subject', 'Content ID', 'Content Name',
                                 'Mime Type', 'Created On', 'Creator (User Name)', 'Last Published On',
                                 'Average Rating(out of 5)']
        content_model['Content ID'] = content_model['Content ID'].str.replace(".img", "")
        content_model['Created On'] = content_model['Created On'].fillna('T').apply(
            lambda x: '-'.join(x.split('T')[0].split('-')[::-1]))
        content_model['Last Published On'] = content_model['Last Published On'].fillna('T').apply(
            lambda x: '-'.join(x.split('T')[0].split('-')[::-1]))
        # content_model['Last Updated On'] = content_model['Last Updated On'].fillna('T').apply(
        #     lambda x: '-'.join(x.split('T')[0].split('-')[::-1]))
        df = content_model.join(df.set_index('identifier'), on='Content ID', how='left')
        df['Last Date of the week'] = (date_ - timedelta(days=1)).strftime('%d-%m-%Y')
        df['Total No of Plays (App and Portal)'] = df['Total No of Plays (App and Portal)'].fillna(0)
        df['Number of Plays on App'] = df['Number of Plays on App'].fillna(0)
        df['Number of Plays on Portal'] = df['Number of Plays on Portal'].fillna(0)
        df['Average Play Time in mins (On App and Portal)'] = df[
            'Average Play Time in mins (On App and Portal)'].fillna(0)
        df['Average Play Time in mins on App'] = df['Average Play Time in mins on App'].fillna(0)
        df['Average Play Time in mins on Portal'] = df['Average Play Time in mins on Portal'].fillna(
            0)
        df = df.fillna('Unknown')
        df.sort_values(inplace=True, ascending=[1, 1, 1, 1, 1, 0],
                       by=['channel', 'Board', 'Medium', 'Grade', 'Subject', 'Total No of Plays (App and Portal)'])
        df.to_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'weekly_plays.csv'), index=False)
        post_data_to_blob(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'weekly_plays.csv'), backup=True)
        for channel in df.channel.unique():
            try:
                slug = tenant_info.loc[channel]['slug']
                print(slug)
            except KeyError:
                continue
            content_aggregates = df[df['channel'] == channel]
            content_aggregates.drop(['channel'], axis=1, inplace=True)
            try:
                get_data_from_blob(result_loc_.parent.joinpath('portal_dashboards', slug, 'content_aggregates.csv'))
                blob_data = pd.read_csv(result_loc_.parent.joinpath('portal_dashboards', slug, 'content_aggregates.csv'))
            except AzureMissingResourceHttpError:
                blob_data = pd.DataFrame()
            except FileNotFoundError:
                blob_data = pd.DataFrame()
            content_aggregates = content_aggregates.append(blob_data).drop_duplicates(
                subset=['Content ID', 'Last Date of the week'], keep='first')
            content_aggregates = content_aggregates[
                ['Board', 'Medium', 'Grade', 'Subject', 'Content ID', 'Content Name', 'Mime Type', 'Created On',
                 'Creator (User Name)', 'Last Published On', 'Total No of Plays (App and Portal)',
                 'Number of Plays on App', 'Number of Plays on Portal', 'Average Play Time in mins (On App and Portal)',
                 'Average Play Time in mins on App', 'Average Play Time in mins on Portal', 'Average Rating(out of 5)',
                 'Last Date of the week']]
            result_loc_.parent.joinpath('portal_dashboards', slug).mkdir(exist_ok=True)
            content_aggregates.to_csv(result_loc_.parent.joinpath('portal_dashboards', slug, 'content_aggregates.csv'),
                                      index=False, encoding='utf-8-sig')
            create_json(result_loc_.parent.joinpath('portal_dashboards', slug, 'content_aggregates.csv'))
            post_data_to_blob(result_loc_.parent.joinpath('portal_dashboards', slug, 'content_aggregates.csv'))


    def init(self):
        start_time_sec = int(round(time.time()))
        print("Content Consumption Report::Start")
        self.data_store_location = Path(self.data_store_location)
        org_search = self.org_search
        druid = self.druid_hostname
        cassandra = self.cassandra_host
        keyspace = self.keyspace_prefix + 'content_db'
        execution_date = datetime.strptime(self.execution_date, "%d/%m/%Y")
        result_loc = self.data_store_location.joinpath('content_plays')
        result_loc.parent.joinpath('portal_dashboards').mkdir(exist_ok=True)
        result_loc.parent.joinpath('config').mkdir(exist_ok=True)
        get_data_from_blob(result_loc.parent.joinpath('config', 'diksha_config.json'))
        with open(result_loc.parent.joinpath('config', 'diksha_config.json'), 'r') as f:
            self.config = json.loads(f.read())
        get_tenant_info(result_loc_=result_loc, org_search_=org_search, date_=execution_date)
        get_content_model(result_loc_=result_loc, druid_=druid, date_=execution_date)
        self.get_tb_content_mapping(result_loc_=result_loc,
                                    content_search_=self.content_search)

        print("Success::Overall Content Consumption Report")
        self.get_overall_report(result_loc_=result_loc, date_=execution_date)
        # 2.9.0
        # self.define_keyspace(cassandra_=cassandra, keyspace_=keyspace)
        # for i in range(7):
        #     analysis_date = execution_date - timedelta(days=i)
        #     get_content_plays(result_loc_=result_loc, date_=analysis_date, druid_=druid)
        #     self.insert_data_to_cassandra(result_loc_=result_loc, date_=analysis_date, cassandra_=cassandra, keyspace_=keyspace)
        # self.get_weekly_plays(result_loc_=result_loc, date_=execution_date, cassandra_=cassandra, keyspace_=keyspace)
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
