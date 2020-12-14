"""
Course consumption report
"""
import json
import os
import sys, time
from datetime import date, datetime, timedelta
from pathlib import Path

import argparse
import findspark
import pandas as pd
from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
from pyspark.sql import functions as func

util_path = os.path.abspath(os.path.join(__file__, '..', '..', '..', 'util'))
sys.path.append(util_path)

from utils import get_tenant_info, create_json, get_data_from_blob, post_data_to_blob, get_courses, push_metric_event

findspark.init()


def get_course_plays(result_loc_, date_):
    """
    Query de-normalised WFS for content play filtered by course id at rollup L1
    :param result_loc_: pathlib.Path object to store resultant CSV at.
    :param date_: datetime object to use in query and path
    :return: None
    """
    courses = pd.read_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'courses.csv'), dtype=str)
    spark = SparkSession.builder.appName("course_plays").master("local[*]").getOrCreate()
    account_name = os.environ['AZURE_STORAGE_ACCOUNT']
    account_key = os.environ['AZURE_STORAGE_ACCESS_KEY']
    container = 'telemetry-data-store'
    spark.conf.set('fs.azure.account.key.{}.blob.core.windows.net'.format(account_name), account_key)
    path = 'wasbs://{}@{}.blob.core.windows.net/telemetry-denormalized/summary/{}-*'.format(container, account_name,
                                                                                            date_.strftime('%Y-%m-%d'))
    data = spark.read.json(path).filter(
        func.col("dimensions.pdata.id").isin(config['context']['pdata']['id']['app'],
                                             config['context']['pdata']['id']['portal']) &
        func.col("dimensions.type").isin("content") &
        func.col('dimensions.mode').isin('play') &
        func.col("object.rollup.l1").isin(courses.identifier.unique().tolist())
    )
    df = data.groupby(
        func.col("object.rollup.l1").alias('courseId'),
        func.col("uid").alias('userId')
    ).agg(
        (func.sum("edata.eks.time_spent") / 60).alias('timespent')
    ).toPandas()
    spark.stop()
    df['Date'] = date_.strftime('%Y-%m-%d')
    df.to_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'course_plays.csv'), index=False)
    post_data_to_blob(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'course_plays.csv'), backup=True)


def get_user_courses(result_loc_, date_, elastic_search_):
    """
    Query elastic search for Course Name based on Course ID and User ID
    :param result_loc_: pathlib.Path object to store resultant CSV at.
    :param date_: datetime object to use in path
    :param elastic_search_: ip and port of the service hosting Elastic Search
    :return: None
    """
    es = Elasticsearch(hosts=[elastic_search_])
    course_plays = pd.read_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'course_plays.csv')).set_index(
        ['courseId', 'userId'])
    users = []
    for ind, row in course_plays.iterrows():
        res = es.search(
            index='user-courses',
            body={
                "query": {"bool": {"filter": [{"term": {"courseId.raw": ind[0]}}, {"term": {"userId.raw": ind[1]}}]}}},
            size=1)
        users.append(pd.DataFrame([x['_source'] for x in res['hits']['hits']]))
        if res['hits']['total'] > 1:
            raise Exception('Response count > 1.')
    user_courses = pd.concat(users)[['courseId', 'userId', 'batchId']]
    user_courses = course_plays.join(user_courses.set_index(['courseId', 'userId']), how='left').dropna(
        subset=['batchId'])
    user_courses = user_courses.groupby(['Date', 'courseId', 'batchId'])['timespent'].sum().round(2)
    user_courses.to_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'user_courses.csv'), header=True)
    post_data_to_blob(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'user_courses.csv'), backup=True)


def get_course_batch(result_loc_, date_, elastic_search_):
    """
    Query elastic search for Batch Name based on Course ID
    :param result_loc_: pathlib.Path object to store resultant CSV at.
    :param date_: datetime object to use in path
    :param elastic_search_: ip and port of the service hosting Elastic Search
    :return: None
    """
    es = Elasticsearch(hosts=[elastic_search_])
    user_courses = pd.read_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'user_courses.csv'), dtype=str)
    course_batch = []
    for ind, row in user_courses.iterrows():
        res = es.search(
            index='course-batch',
            body={"query": {"bool": {
                "filter": [{"term": {"courseId.raw": row['courseId']}}, {"term": {"batchId.raw": row['batchId']}}]}}})
        course_batch.append(pd.DataFrame([x['_source'] for x in res['hits']['hits']]))
        if res['hits']['total'] > 1:
            raise Exception('Response count > 1.')
    course_batch = pd.concat(course_batch)[['courseId', 'batchId', 'status', 'name']]
    course_batch = user_courses.join(course_batch.set_index(['courseId', 'batchId']), on=['courseId', 'batchId'],
                                     how='left')
    course_batch.status = course_batch.status.apply(
        lambda x: 'Upcoming' if x == 0 else 'Ongoing' if x == 1 else 'Expired' if x == 2 else '')
    courses = pd.read_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'courses.csv'), dtype=str)
    course_batch = course_batch.join(courses.set_index('identifier'), on='courseId', how='left', rsuffix='_r')
    course_batch.rename({'name_r': 'Course Name', 'name': 'Batch Name', 'status': 'Batch Status',
                         'timespent': 'Total Timespent (in mins)'}, axis=1, inplace=True)
    course_batch.drop(['batchId', 'courseId'], axis=1)[
        ['Date', 'channel', 'Course Name', 'Batch Name', 'Batch Status', 'Total Timespent (in mins)']].to_csv(
        result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'course_batch.csv'), index=False)
    post_data_to_blob(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'course_batch.csv'), backup=True)


def generate_course_usage(result_loc_, date_):
    """
    Generate course consumption report.
    :param result_loc_: pathlib.Path object to store resultant CSV at.
    :param date_: datetime object to use in path
    :return: None
    """
    tenant_info = pd.read_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'tenant_info.csv'), dtype=str)[
        ['id', 'slug']].set_index('id')
    course_batch = pd.read_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'course_batch.csv'), dtype=str)
    for channel in course_batch.channel.unique():
        try:
            slug = tenant_info.loc[channel][0]
            print(slug)
            df = course_batch[course_batch['channel'] == channel]
            result_loc_.parent.joinpath('portal_dashboards', slug).mkdir(exist_ok=True)
            try:
                get_data_from_blob(result_loc_.parent.joinpath('portal_dashboards', slug, 'course_usage.csv'))
                blob_data = pd.read_csv(result_loc_.parent.joinpath('portal_dashboards', slug, 'course_usage.csv'))
            except:
                blob_data = pd.DataFrame()
            blob_data = blob_data.append(df, sort=False).fillna('')
            blob_data.drop_duplicates(subset=['Date', 'Course Name', 'Batch Name', 'Batch Status'], inplace=True,
                                      keep='last')
            blob_data.sort_values(['Date', 'Course Name', 'Batch Name', 'Batch Status'], ascending=False, inplace=True)
            blob_data.drop('channel', axis=1).to_csv(
                result_loc_.parent.joinpath('portal_dashboards', slug, 'course_usage.csv'), index=False)
            create_json(result_loc_.parent.joinpath('portal_dashboards', slug, 'course_usage.csv'))
            post_data_to_blob(result_loc_.parent.joinpath('portal_dashboards', slug, 'course_usage.csv'))
        except KeyError:
            print(channel, 'channel not in tenant list')


start_time_sec = int(round(time.time()))
parser = argparse.ArgumentParser()
parser.add_argument("data_store_location", type=str, help="data folder location")
parser.add_argument("org_search", type=str, help="host address for Org API")
parser.add_argument("Druid_hostname", type=str, help="host address for Druid API")
parser.add_argument("elastic_search", type=str, help="host address for API")
parser.add_argument("-execution_date", type=str, default=date.today().strftime("%d/%m/%Y"),
                    help="DD/MM/YYYY, optional argument for backfill jobs")
args = parser.parse_args()
result_loc = Path(args.data_store_location).joinpath('course_dashboards')
result_loc.mkdir(exist_ok=True)
org_search = args.org_search
druid_ip = args.Druid_hostname
elastic_search = args.elastic_search
execution_date = datetime.strptime(args.execution_date, "%d/%m/%Y")
analysis_date = execution_date - timedelta(days=1)
result_loc.joinpath(analysis_date.strftime('%Y-%m-%d')).mkdir(exist_ok=True)
result_loc.parent.joinpath('config').mkdir(exist_ok=True)
get_data_from_blob(result_loc.parent.joinpath('config', 'diksha_config.json'))
with open(result_loc.parent.joinpath('config', 'diksha_config.json'), 'r') as f:
    config = json.loads(f.read())
get_tenant_info(result_loc_=result_loc, org_search_=org_search, date_=analysis_date)
get_courses(result_loc_=result_loc, druid_ip_=druid_ip, date_=analysis_date)
get_course_plays(result_loc_=result_loc, date_=analysis_date)
get_user_courses(result_loc_=result_loc, date_=analysis_date, elastic_search_=elastic_search)
get_course_batch(result_loc_=result_loc, date_=analysis_date, elastic_search_=elastic_search)
generate_course_usage(result_loc_=result_loc, date_=analysis_date)

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
push_metric_event(metrics, "Course Consumption Report")