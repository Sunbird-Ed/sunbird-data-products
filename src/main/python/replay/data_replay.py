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
from replay_utils import push_data, getDates, getBackUpDetails, getKafkaTopic, getInputPrefix, restoreBackupData, backupData, deleteBackupData, getFilterStr, getFilterDetails
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
    filterString = getFilterStr(getFilterDetails(config_json, prefix))
    for date in dateRange:
        try:
            input_prefix = getInputPrefix(config_json, prefix)
            if delete_backups == "True":
                sinkSourcesList = getBackUpDetails(config_json, prefix)
                # take backups before replay
                print("Taking backups before starting replay")
                copy_data(container, input_prefix, 'backup-{}'.format(input_prefix), date)
                delete_data(container, input_prefix, date)
                backupData(sinkSourcesList, container, date)
                print("Taking backups completed. Starting data replay")
                kafkaTopic = getKafkaTopic(config_json, prefix)
                try:
                    backup_prefix = 'backup-{}'.format(input_prefix)
                    push_data(kafka_broker_list, kafkaTopic, container, backup_prefix, date, filterString)
                    print("Data replay completed")
                except Exception:
                    #restore backups if replay fails 
                    print("Error while data replay, restoring backups") 
                    copy_data(container, 'backup-{}'.format(input_prefix), input_prefix, date)
                    delete_data(container, 'backup-{}'.format(input_prefix), date)
                    restoreBackupData(sinkSourcesList, container, date)
                    print("Error while data replay, backups restored") 
                    log.exception()        
                    raise  
                # delete backups and disable segments after replay
                print("Data replay completed. Deleting backups and druid segments")
                delete_data(container, 'backup-{}'.format(input_prefix), date)
                deleteBackupData(sinkSourcesList, container, date)
                print("Data replay completed. Deleted backups and druid segments")   
            else:
                if "failed" in prefix:
                    kafkaTopic = getKafkaTopic(config_json, prefix)
                    push_data(kafka_broker_list, kafkaTopic, container, prefix, date, filterString)
                else:  
                    backup_dir = 'backup-{}'.format(input_prefix)
                    copy_data(container, input_prefix, backup_dir, date)
                    delete_data(container, input_prefix, date)
                    kafkaTopic = getKafkaTopic(config_json, prefix)
                    try:
                        push_data(kafka_broker_list, kafkaTopic, container, backup_dir, date, filterString)
                        print("Data replay completed")
                    except Exception: 
                        print("Error while data replay, restoring backups")
                        copy_data(container, 'backup-{}'.format(input_prefix), input_prefix, date)
                        delete_data(container, 'backup-{}'.format(input_prefix), date) 
        except Exception:
            print("Replay failed for {}. Continuing replay for remaining dates".format(date.strftime('%Y-%m-%d')))
            log.exception()
            pass
except Exception:
        raise







