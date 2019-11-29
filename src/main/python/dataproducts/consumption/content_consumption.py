"""
content level plays, timespent and ratings by week
"""
import json
import sys, time
from datetime import datetime, timedelta, date
from pathlib import Path
from string import Template
import pdb
import os

import argparse
import pandas as pd
from azure.common import AzureMissingResourceHttpError
from cassandra.cluster import Cluster

util_path = os.path.abspath(os.path.join(__file__, '..', '..', '..', 'util'))
sys.path.append(util_path)

from utils import create_json, get_tenant_info, get_data_from_blob, \
    post_data_to_blob, get_content_model, get_content_plays, push_metric_event


def mime_type(series):
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


def define_keyspace(cassandra_, keyspace_, replication_factor_=1):
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


def insert_data_to_cassandra(result_loc_, date_, cassandra_, keyspace_):
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


def get_weekly_plays(result_loc_, date_, cassandra_, keyspace_):
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
        pdata_id = 'App' if row.pdata_id == config['context']['pdata']['id']['app'] else 'Portal' if \
            row.pdata_id == config['context']['pdata']['id']['portal'] else 'error'
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
    # df = df.fillna(0)
    df = df[['identifier', 'Total No of Plays (App and Portal)', 'Number of Plays on App', 'Number of Plays on Portal',
             'Average Play Time in mins (On App and Portal)',
             'Average Play Time in mins on App',
             'Average Play Time in mins on Portal']]
    content_model = pd.read_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'content_model_snapshot.csv'))[
        ['channel', 'board', 'medium', 'gradeLevel', 'subject', 'identifier', 'name', 'mimeType', 'createdOn', 'creator',
         'lastPublishedOn', 'me_averageRating']]
    content_model["creator"] = content_model["creator"].str.replace("null", "")
    content_model['channel'] = content_model['channel'].astype(str)
    content_model['mimeType'] = content_model['mimeType'].apply(mime_type)
    content_model.columns = ['channel', 'Board', 'Medium', 'Grade', 'Subject', 'Content ID', 'Content Name',
                             'Mime Type', 'Created On', 'Creator (User Name)', 'Last Published On',
                             'Average Rating(out of 5)']
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


start_time_sec = int(round(time.time()))
print("Content Consumption Report::Start")
parser = argparse.ArgumentParser()
parser.add_argument("--data_store_location", type=str, help="the path to local data folder")
parser.add_argument("--org_search", type=str, help="host address for Org API")
parser.add_argument("--druid_hostname", type=str, help="Host address for Druid")
parser.add_argument("--cassandra_host", type=str, help="Host address for Cassandra")
parser.add_argument("--keyspace_prefix", type=str, help="Environment for keyspace in Cassandra")
parser.add_argument("--execution_date", type=str, default=date.today().strftime("%d/%m/%Y"),
                    help="DD/MM/YYYY, optional argument for backfill jobs")
args = parser.parse_args()
data_store_location = Path(args.data_store_location)
org_search = args.org_search
druid = args.druid_hostname
cassandra = args.cassandra_host
keyspace = args.keyspace_prefix + 'content_db'
execution_date = datetime.strptime(args.execution_date, "%d/%m/%Y")
result_loc = data_store_location.joinpath('content_plays')
result_loc.parent.joinpath('portal_dashboards').mkdir(exist_ok=True)
result_loc.parent.joinpath('config').mkdir(exist_ok=True)
get_data_from_blob(result_loc.parent.joinpath('config', 'diksha_config.json'))
with open(result_loc.parent.joinpath('config', 'diksha_config.json'), 'r') as f:
    config = json.loads(f.read())
get_tenant_info(result_loc_=result_loc, org_search_=org_search, date_=execution_date)
get_content_model(result_loc_=result_loc, druid_=druid, date_=execution_date)
define_keyspace(cassandra_=cassandra, keyspace_=keyspace)
for i in range(7):
    analysis_date = execution_date - timedelta(days=i)
    get_content_plays(result_loc_=result_loc, date_=analysis_date, druid_=druid)
    insert_data_to_cassandra(result_loc_=result_loc, date_=analysis_date, cassandra_=cassandra, keyspace_=keyspace)
get_weekly_plays(result_loc_=result_loc, date_=execution_date, cassandra_=cassandra, keyspace_=keyspace)
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
        "value": datetime.strptime(args.execution_date, "%Y-%m-%d")
    }
]
push_metric_event(metrics, "Content Consumption Metrics")