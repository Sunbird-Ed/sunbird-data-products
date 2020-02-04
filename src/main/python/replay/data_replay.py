import os
import sys
import findspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from datetime import date, timedelta, datetime
import argparse
from pathlib import Path

util_path = os.path.abspath(os.path.join(__file__, '..', 'util'))
sys.path.append(util_path)
resources_path = os.path.abspath(os.path.join(__file__, '..', 'resources'))
sys.path.append(resources_path)

from azure_utils import copy_data, delete_data
from postgres_utils import executeQuery
from replay_utils import push_data, getDates, getBackUpDetails, getKafkaTopic
import replay_config


start_time = datetime.now()
print("Started at: ", start_time.strftime('%Y-%m-%d %H:%M:%S'))
parser = argparse.ArgumentParser()
parser.add_argument("container", type=str, help="the data container")
parser.add_argument("prefix", type=str, help="replay data prefix")
parser.add_argument("start_date", type=str, help="YYYY-MM-DD, replay start date")
parser.add_argument("end_date", type=str, help="YYYY-MM-DD, replay end date")
parser.add_argument("kafka_broker_list", type=str, help="kafka broker details")
#parser.add_argument("kafka_topic", type=str, help="kafka topic")
parser.add_argument("delete_backups", type=str, default="False", help="boolean flag whether to delete backups")

args = parser.parse_args()
container = args.container
prefix = args.prefix
start_date = args.start_date
end_date = args.end_date
kafka_broker_list = args.kafka_broker_list
delete_backups = args.delete_backups

config_json = replay_config.init()

#pgGetSegmentsQuery = "select * from druid_segments where created_date >= '{}' and created_date <= '{}' and datasource = '{}' and used='t'"
pgDisableSegmentsQuery = "update druid_segments set used='f' where created_date >= '{}' and created_date <= '{}' and datasource = '{}' and used='t'"

#if delete backups is true
# copy_data for from all data backups
# delete_data from all data backups
# get druid segments for the date range
# load_data from prefix and push to kafka topic
# delete_data backup took at the start
# mark druid segments for deletion

#if delete backups is false
# load_data from prefix and push to kafka topic

dateRange = getDates(start_date, end_date)
print(dateRange)
print(delete_backups)
try:
    for date in dateRange:
        try:
            if delete_backups == "True":
                sinkSourcesList = getBackUpDetails(config_json, prefix)
                # take backups before replay
                for sink in sinkSourcesList:
                    if sink['type'] == 'azure':
                        backup_dir = 'backup-{}'.format(sink['prefix'])
                        copy_data(container, sink['prefix'], backup_dir, date)
                        delete_data(container, sink['prefix'], date)
                    if sink['type'] == 'druid':
                        end_date = date + timedelta(1)
                        # pgGetSegmentsQuery.format(date.strftime('%Y-%m-%d %H:%M:%S'), end_date.strftime('%Y-%m-%d %H:%M:%S'), sink['prefix'])
                        # segments = executeQuery('sowmya', '', 'localhost', 'postgis_test', pgGetSegmentsQuery)
                kafkaTopic = getKafkaTopic(config_json, prefix)
                push_data(kafka_broker_list, kafkaTopic, container, backup_dir, date)
                # delete backups and disable segments after replay
                for sink in sinkSourcesList:
                    if sink['type'] == 'azure':
                        backup_dir = 'backup-{}'.format(sink['prefix'])
                        delete_data(container, backup_dir, date)
                    if sink['type'] == 'druid':
                        end_date = date + timedelta(1)
                        pgDisableSegmentsQuery.format(date.strftime('%Y-%m-%d %H:%M:%S'), end_date.strftime('%Y-%m-%d %H:%M:%S'), sink['prefix'])
                        segments = executeQuery('sowmya', '', 'localhost', 'postgis_test', pgDisableSegmentsQuery)
            else:
                backup_dir = 'backup-{}'.format(prefix)
                # copy_data(container, sink['prefix'], backup_dir, date)
                # delete_data(container, sink['prefix'], date)
                kafkaTopic = getKafkaTopic(config_json, prefix)
                push_data(kafka_broker_list, kafkaTopic, container, backup_dir, date)
        except Exception:
            print("Replay failed for {}. Continuing replay for remaining dates".format(date.strftime('%Y-%m-%d')))
            raise
except Exception:
        raise







