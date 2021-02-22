#!anomaly_venv/bin/python
import logging

from flask import Flask, request
import requests
import json
import jsons

import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML

from dataclasses import dataclass, make_dataclass, field

import ciso8601
import datetime
from datetime import date, datetime as dtime

from typing import List

from dataclasses_json import dataclass_json

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

@dataclass
class QueryDate:
    start_time: datetime
    end_time: datetime

@dataclass
class QueryColumns:
    dimensions: List[str]
    metric: str

@dataclass
class DruidQuery:
    query: str

@dataclass_json
@dataclass
class Dimension:
    id: str
    value: str

@dataclass_json
@dataclass
class Metric:
    id: str
    value: float

@dataclass_json
@dataclass
class QueryResult:
    dimensions: List[Dimension]
    metric: Metric

    def __eq__(self, other):
        if not isinstance(other, QueryResult):
            return NotImplemented

        return self.dimensions == other.dimensions

# Job datastructure
# job_id: unique id for the job
# query_granularity: Time granularity for aggregating results from the Druid query output
# query: Query string to be executed
# offset_in_mins: Offset to account for delay in data
# frequency: Frequency of execution of the job
# cron: Cron Expression to express the frequency of the job. Takes precedence over the "frequency" field
@dataclass
class Job:
    job_id: str
    query_granularity: str
    query: str
    frequency: str
    offset_in_mins: int
    cron: str = ""

@dataclass
class Jobs:
    jobs: List[Job]

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logging.getLogger('apscheduler').setLevel(logging.INFO)

druid_host = "localhost"
druid_broker_port = "8082"
druid_endpoint = "druid/v2/sql"
execution_jobs = []
sql_agg_fn_list = ['SUM', 'AVG', 'MAX', 'MIN', '*', '/']
data_dir = "/home/analytics/anomaly_detection"

granularity = {
    'day': lambda x: x * 24 * 60 * 60,
    'second': lambda x: x * 1,
    'minute': lambda x: x * 60,
    'hour': lambda x: x * 60 * 60
}

# config.json is expected to be present in the same directory as
# this script. e.g.:
# {
#   "jobs": [
#     {
#       "job_id": "job-1",
#       "query_granularity": "hour",
#       "query": "query",
#       "cron": "*/1 * * * *",
#       "frequency": "minute",
#       "offset_in_mins": 120
#     }
#   ]
# }
try:
    with open('config.json') as config_file:
        json_dict = json.load(config_file)
        execution_jobs = jsons.load(json_dict, Jobs)
except IOError:
    logging.error("Job config.json not found in the root directory...")
    raise IOError

# Function to compute start_time and end_time based on the job configuration
def compute_dates(execution_time, query_granularity = 'day', offset_in_mins=0, current_interval = True):
    interval = granularity.get(query_granularity,(24*60*60))(1)
    if current_interval:
        start_time = execution_time - datetime.timedelta(seconds=interval + offset_in_mins*60)
        end_time = execution_time - datetime.timedelta(seconds=offset_in_mins*60)
    else:
        start_time = execution_time - 2 * datetime.timedelta(seconds=interval + offset_in_mins*60)
        end_time = execution_time - datetime.timedelta(seconds=interval + offset_in_mins*60)
    return QueryDate(start_time, end_time)

# Function to execute Druid query and parse dimensions, metric values
def query_druid(druid_query, start_time, end_time):
    logging.info("Query start_time: {}, end_time: {}".format(start_time, end_time))
    json_payload = {}
    json_payload['query'] = druid_query.format(start_time=start_time, end_time=end_time)
    query = json_payload['query']
    logging.debug("Exectuting query: {}".format(query))
    columns = parse_sql_columns(query)
    
    headers = {'Content-Type': 'application/json; charset=utf-8'}
    response = requests.post('http://{}:{}/{}'.format(druid_host, druid_broker_port, druid_endpoint), data=json.dumps(json_payload), headers=headers)
    query_result = parseDruidResponse(response.json(), columns)
    return query_result

# Function to parse name of the dimensions and metrics from the Druid query
def parse_sql_columns(sql):
    dimensions = []
    parsed = sqlparse.parse(sql)
    stmt = parsed[0]
    for token in stmt.tokens:
        if isinstance(token, IdentifierList):
            for identifier in token.get_identifiers():
                is_metric = [agg_fn for agg_fn in sql_agg_fn_list if(agg_fn in str(identifier))]
                if is_metric:
                    metric = identifier.get_name()
                else:
                    dimensions.append(identifier.get_name())
        if isinstance(token, Identifier):
            pass
        if token.ttype is Keyword:  # from
            break
    return QueryColumns(dimensions, metric)

# Function to parse query response from Druid according to the dimensions and metric in the query
def parseDruidResponse(response_json, columns):
    dimensions = columns.dimensions
    metric = columns.metric

    query_result = list()
    for elem in response_json:
        dimensions_values_list = list()
        for dim in dimensions:
            dimensions_values_list.append(Dimension(id=dim, value=elem[dim]))
        query_result.append(QueryResult(dimensions_values_list, Metric(id=metric, value=round(elem[metric], 2))))
    return query_result

# Function to compute percentage difference between current interval and 
# previous interval
def compare_with_previous_interval(job):
    logging.info("Exectuting {} job".format(job.job_id))
    execution_time = dtime.now().replace(minute=0, second=0, microsecond=0)
    # execution_time = ciso8601.parse_datetime('2020-12-17')
    current_datetime = compute_dates(execution_time, job.query_granularity, offset_in_mins=job.offset_in_mins)
    previous_datetime = compute_dates(execution_time, job.query_granularity, offset_in_mins=job.offset_in_mins, current_interval=False) 

    current_result = query_druid(job.query, current_datetime.start_time, current_datetime.end_time)
    previous_result = query_druid(job.query, previous_datetime.start_time, previous_datetime.end_time)

    data_points_list = find_result_intersection(current_result, previous_result)

    result = QueryResult.schema().dumps(data_points_list, many=True)
    write_result(data_points_list)
    logging.info("Execution of {} job completed".format(job.job_id))
    return result

def find_result_intersection(current_result, previous_result):
    data_points_list = list()
    for res in current_result:
        index = previous_result.index(res) if res in previous_result else -1
        if (index != -1):
            prev_res = previous_result[index]
            cur_val = res.metric.value
            prev_val = prev_res.metric.value
            if (prev_val != 0):
                perc_diff = (cur_val - prev_val) * 100.0/prev_val
            else:
                perc_diff = 100.0
            data_points_list.append(
                createResult(res.dimensions, Metric(res.metric.id, round(perc_diff, 2))))
        else:
            data_points_list.append(
                createResult(res.dimensions, Metric(res.metric.id, 100.0)))
    return data_points_list

# Function to fetch data for the Druid query for the current interval
def fetch_data_for_interval(job):
    logging.info("Exectuting {} job".format(job.job_id))
    data_points_list = list()
    execution_time = dtime.now().replace(minute=0, second=0, microsecond=0)
    execution_times = compute_dates(execution_time, job.query_granularity, offset_in_mins=job.offset_in_mins)
    result = query_druid(job.query, execution_times.start_time, execution_times.end_time)

    for res in result:
        data_points_list.append(
                createResult(res.dimensions, Metric(res.metric.id, round(res.metric.value, 2))))
    result = QueryResult.schema().dumps(data_points_list, many=True)
    write_result(data_points_list)
    logging.info("Execution of {} job completed".format(job.job_id))

def write_result(query_result):
    with open("{}/anomaly_data.log".format(data_dir), "a") as data_file:
        for data in query_result:
            data_file.write(f'{QueryResult.schema().dumps(data)}\n')

# Script entry point to schedule jobs from configuration
def execute():
    logging.info("Starting scheduling of jobs...")
    scheduler = BlockingScheduler()
    for job in execution_jobs.jobs:
        if len(job.cron) != 0:
            logging.info("cron expression: {}".format(job.cron))
            scheduler.add_job(fetch_data_for_interval, CronTrigger.from_crontab(job.cron), args=[job])
        else:
            if job.frequency == 'day':
                scheduler.add_job(fetch_data_for_interval, trigger='cron', args=[job], hour='04', minute='00')
            
            if job.frequency == 'hour':
                scheduler.add_job(fetch_data_for_interval, 'interval', args=[job], hours=1)

            if job.frequency == 'minute':
                scheduler.add_job(fetch_data_for_interval, 'interval', args=[job], minutes=1)
    logging.info("Scheduling jobs completed...")
    scheduler.start()

def createResult(dimensions, metric):
    return QueryResult(dimensions=dimensions, metric=metric)

if __name__ == '__main__':
    execute()