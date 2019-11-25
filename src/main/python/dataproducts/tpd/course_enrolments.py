"""
generate course related enrolment and completion report.
"""
import sys, time
from datetime import date, datetime
from pathlib import Path

import argparse
import pandas as pd
from elasticsearch import Elasticsearch

sys.path.append(Path(__file__).parent.parent.parent.parent.parent.parent)

from src.main.python.util.utils import get_tenant_info, create_json, post_data_to_blob, get_courses, push_metric_event


def get_course_enrollments(result_loc_, elastic_search_, date_, size_=1000):
    """
    Query course batch index for batch name
    :param result_loc_: pathlib.Path object to store resultant CSV at.
    :param elastic_search_: ip and port of service hosting Elastic Search
    :param date_: datetime object for date to use in path
    :param size_: query limit size
    :return: None
    """
    es = Elasticsearch(hosts=[elastic_search_])
    res1 = es.search(index='course-batch', body={"query": {"match_all": {}}}, size=size_)
    course_batch = pd.DataFrame([x['_source'] for x in res1['hits']['hits']])[
        ['batchId', 'courseId', 'name', 'status', 'participantCount', 'completedCount']]
    course_batch.to_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'course_batch.csv'), index=False)
    post_data_to_blob(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'course_batch.csv'), backup=True)
    courses = pd.read_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'courses.csv'))
    df = course_batch.join(courses.set_index('identifier'), on='courseId', how='inner', rsuffix='_r').fillna(0)
    df.status = df.status.apply(lambda x: 'Ongoing' if x == 1 else 'Expired' if x == 2 else '')
    df = df[['channel', 'name_r', 'name', 'status', 'participantCount', 'completedCount']]
    df.columns = ['channel', 'Course Name', 'Batch Name', 'Batch Status', 'Enrolment Count', 'Completion Count']
    board_slug = pd.read_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), 'tenant_info.csv'))[['id', 'slug']]
    board_slug.set_index('id', inplace=True)
    for channel in df.channel.unique():
        try:
            slug = board_slug.loc[channel][0]
            df1 = df[df['channel'] == channel]
            df1.drop(['channel'], axis=1, inplace=True)
            result_loc_.joinpath(date_.strftime('%Y-%m-%d'), slug).mkdir(exist_ok=True)
            df1.to_csv(result_loc_.joinpath(date_.strftime('%Y-%m-%d'), slug, 'course_enrollments.csv'), index=False)
            create_json(read_loc_=result_loc_.joinpath(date_.strftime('%Y-%m-%d'), slug, 'course_enrollments.csv'))
            post_data_to_blob(
                result_loc_=result_loc_.joinpath(date_.strftime('%Y-%m-%d'), slug, 'course_enrollments.csv'))
        except KeyError:
            pass


start_time_sec = int(round(time.time()))
parser = argparse.ArgumentParser()
parser.add_argument("data_store_location", type=str, help="data folder location")
parser.add_argument("org_search", type=str, help="host address for Org API")
parser.add_argument("Druid_hostname", type=str, help="host address for Druid API")
parser.add_argument("elastic_search", type=str, help="host address for API")
parser.add_argument("response_size", type=int, help="max response size limiter", default=1000)
parser.add_argument("-execution_date", type=str, default=date.today().strftime("%d/%m/%Y"),
                    help="DD/MM/YYYY, optional argument for backfill jobs")
args = parser.parse_args()
result_loc = Path(args.data_store_location).joinpath('course_dashboards')
result_loc.mkdir(exist_ok=True)
org_search = args.org_search
druid_ip = args.Druid_hostname
elastic_search = args.elastic_search
resp_size = args.response_size
execution_date = datetime.strptime(args.execution_date, "%d/%m/%Y")
get_tenant_info(result_loc_=result_loc, org_search_=org_search, date_=execution_date)
get_courses(result_loc_=result_loc, druid_=druid_ip, query_file_='course_list.json', date_=execution_date)
get_course_enrollments(result_loc_=result_loc, elastic_search_=elastic_search, date_=execution_date, size_=resp_size)

end_time_sec = int(round(time.time()))
time_taken = end_time_sec - start_time_sec
metrics = {
    "system": "AdhocJob",
    "subsystem": "Course Enrollments Report",
    "metrics": [
        {
            "metric": "timeTakenSecs",
            "value": time_taken
        },
        {
            "metric": "date",
            "value": datetime.strptime(args.execution_date, "%Y-%m-%d")
        }
    ]
}
push_metric_event(metrics, "Course Enrollments Report")