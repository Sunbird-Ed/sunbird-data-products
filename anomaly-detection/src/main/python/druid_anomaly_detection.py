#!/usr/bin/env python
import logging
from logging.handlers import RotatingFileHandler
import traceback

import requests
import json
import jsons

import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML

from dataclasses import dataclass, make_dataclass, field

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


class DruidAnomalyDetection:
    def __init__(self, config_file_location, data_dir, druid_broker_host, druid_broker_port):
        self.config_file_location = config_file_location
        self.data_dir = data_dir
        self.druid_broker_host = druid_broker_host
        self.druid_broker_port = druid_broker_port

    # Function to compute start_time and end_time based on the job configuration
    def compute_dates(self, execution_time, query_granularity = 'day', offset_in_mins=0, current_interval = True):
        interval = self.granularity.get(query_granularity,(24*60*60))(1)
        if current_interval:
            start_time = execution_time - datetime.timedelta(seconds=interval + offset_in_mins*60)
            end_time = execution_time - datetime.timedelta(seconds=offset_in_mins*60)
        else:
            start_time = execution_time - 2 * datetime.timedelta(seconds=interval + offset_in_mins*60)
            end_time = execution_time - datetime.timedelta(seconds=interval + offset_in_mins*60)
        return QueryDate(start_time, end_time)

    # Function to execute Druid query and parse dimensions, metric values
    def query_druid(self, job_id, druid_query, start_time, end_time):
        self.logger.info("JOB_ID: {} Query start_time: {}, end_time: {}".format(job_id, start_time, end_time))
        json_payload = {'query': druid_query.format(start_time=start_time, end_time=end_time)}
        query = json_payload['query']
        self.logger.debug("Exectuting query: {}".format(query))
        columns = self.parse_sql_columns(query)
        self.logger.info("JOB_ID {}, columns: {}".format(job_id, columns))
        
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        query_result = list()
        try:
            response = requests.post(self.druid_endpoint, data=json.dumps(json_payload), headers=headers)
            query_result = self.parse_druid_response(response.json(), columns)
        except Exception:
            logging.error("Error occurred when executing query for job_id {}".format(job_id))
            logging.error("StackTrace: {}".format(traceback.print_exc()))
        return query_result

    # Function to parse name of the dimensions and metrics from the Druid query
    def parse_sql_columns(self, sql):
        dimensions = []
        parsed = sqlparse.parse(sql)
        stmt = parsed[0]
        for token in stmt.tokens:
            if isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    is_metric = [agg_fn for agg_fn in self.sql_agg_fn_list if(agg_fn in str(identifier))]
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
    def parse_druid_response(self, response_json, columns):
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
    def compare_with_previous_interval(self, job):
        self.logger.info("Exectuting {} job".format(job.job_id))
        execution_time = dtime.now().replace(minute=0, second=0, microsecond=0)
        current_datetime = self.compute_dates(execution_time, job.query_granularity, offset_in_mins=job.offset_in_mins)
        previous_datetime = self.compute_dates(execution_time, job.query_granularity, offset_in_mins=job.offset_in_mins, current_interval=False) 

        current_result = self.query_druid(job.query, current_datetime.start_time, current_datetime.end_time)
        previous_result = self.query_druid(job.query, previous_datetime.start_time, previous_datetime.end_time)

        data_points_list = self.find_result_intersection(current_result, previous_result)

        if data_points_list:
            result = QueryResult.schema().dumps(data_points_list, many=True)
            self.write_result(data_points_list)
        self.logger.info("Execution of {} job completed".format(job.job_id))

    def find_result_intersection(self, current_result, previous_result):
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
                    self.create_result(res.dimensions, Metric(res.metric.id, round(perc_diff, 2))))
            else:
                data_points_list.append(
                    self.create_result(res.dimensions, Metric(res.metric.id, 100.0)))
        return data_points_list

    # Function to fetch data for the Druid query for the current interval
    def fetch_data_for_interval(self, job):
        self.logger.info("Exectuting {} job".format(job.job_id))
        data_points_list = list()
        execution_time = dtime.now().replace(minute=0, second=0, microsecond=0)
        execution_times = self.compute_dates(execution_time, job.query_granularity, offset_in_mins=job.offset_in_mins)
        result = self.query_druid(job.job_id, job.query, execution_times.start_time, execution_times.end_time)

        for res in result:
            data_points_list.append(
                    self.create_result(res.dimensions, Metric(res.metric.id, round(res.metric.value, 2))))
        result = QueryResult.schema().dumps(data_points_list, many=True)
        self.write_result(data_points_list)
        self.logger.info("Execution of {} job completed".format(job.job_id))

    def write_result(self, query_result):
        with open("{}/anomaly_data.log".format(self.data_dir), "a") as data_file:
            for data in query_result:
                data_file.write(f'{QueryResult.schema().dumps(data)}\n')

    # Script entry point to schedule jobs from configuration
    def execute(self):
        self.logger.info("Starting scheduling of jobs...")
        scheduler = BlockingScheduler()
        for job in self.execution_jobs.jobs:
            if len(job.cron) != 0:
                self.logger.info("cron expression: {}".format(job.cron))
                scheduler.add_job(self.fetch_data_for_interval, CronTrigger.from_crontab(job.cron), args=[job])
            else:
                if job.frequency == 'day':
                    scheduler.add_job(self.fetch_data_for_interval, trigger='cron', args=[job], hour='04', minute='00')
                
                if job.frequency == 'hour':
                    scheduler.add_job(self.fetch_data_for_interval, 'interval', args=[job], hours=1)

                if job.frequency == 'minute':
                    scheduler.add_job(self.fetch_data_for_interval, 'interval', args=[job], minutes=1)
        self.logger.info("Scheduling jobs completed...")
        scheduler.start()

    def create_result(self, dimensions, metric):
        return QueryResult(dimensions=dimensions, metric=metric)

    def init(self):
        self.logger = logging.getLogger("root")
        self.logger.setLevel(logging.INFO)
        rotating_file_handler = logging.handlers.RotatingFileHandler(
            filename='{}/anomaly_detection-{:%Y-%m-%d}.log'.format(self.data_dir, datetime.datetime.now()),
            mode='a',
            maxBytes=52428800,
            backupCount=10
        )
        rotating_file_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
        self.logger.addHandler(rotating_file_handler)
        logging.getLogger('apscheduler').setLevel(logging.INFO)

        self.granularity = {
            'day': lambda x: x * 24 * 60 * 60,
            'second': lambda x: x * 1,
            'minute': lambda x: x * 60,
            'hour': lambda x: x * 60 * 60
        }
        self.druid_endpoint = "{}:{}/{}".format(self.druid_broker_host, self.druid_broker_port, "druid/v2/sql")
        self.execution_jobs = []
        self.sql_agg_fn_list = ['SUM', 'AVG', 'MAX', 'MIN', '*', '/']

        try:
            with open(self.config_file_location) as config_file:
                json_dict = json.load(config_file)
                self.execution_jobs = jsons.load(json_dict, Jobs)
        except IOError:
            logging.error("Job config.json not found in the root directory...")
            raise IOError