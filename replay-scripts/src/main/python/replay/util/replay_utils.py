import os
import sys
import json
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql import functions as func
from pathlib import Path
from datetime import date, timedelta, datetime
from pyspark.sql.types import StringType
from .azure_utils import copy_data, delete_data, get_data_path
from .postgres_utils import executeQuery
from kafka import KafkaProducer
from kafka.errors import KafkaError

def push_data(broker_host, topic, container, prefix, date, filters):
    findspark.init()
    path = get_data_path(container, prefix, date)
    print(path)
    # path = "wasbs://dev-data-store@sunbirddevtelemetry.blob.core.windows.net/unique/2020-01-01-1577818009896.json.gz"
    account_name = os.environ['AZURE_STORAGE_ACCOUNT']
    account_key = os.environ['AZURE_STORAGE_ACCESS_KEY']
    spark = SparkSession.builder.appName("data_replay").master("local[*]").getOrCreate()
    spark.conf.set('fs.azure.account.key.{}.blob.core.windows.net'.format(account_name), account_key)
    df = spark.read.json(path)
    inputCount = df.count()
    print(inputCount)
    if filters:
        filteredDf = df.filter(filters)
    else :
        filteredDf = df
    print(filteredDf.count())
    def push_data_kafka(events):
        kafka_producer = KafkaProducer(bootstrap_servers=[broker_host])
        for event in events:
            kafka_producer.send(topic, bytearray(event, 'utf-8'))
            kafka_producer.flush()
    filteredDf.toJSON().foreachPartition(push_data_kafka)
    spark.stop()
    

def getDates(start, end):
    dates = []
    start = datetime.strptime(start, "%Y-%m-%d")
    end = datetime.strptime(end, "%Y-%m-%d")
    delta = end - start
    for i in range(delta.days + 1):
        dates.append(start + timedelta(days=i))
    return dates

def getBackUpDetails(config_json, prefix):
    return config_json[prefix]['dependentSinkSources']

def getFilterDetails(config_json, prefix):
    if config_json[prefix].get('filters'):
        return config_json[prefix].get('filters')
    else:
        return ""

def getKafkaTopic(config_json, prefix):
    return config_json[prefix]['outputKafkaTopic']

def getInputPrefix(config_json, prefix):
    return config_json[prefix]['inputPrefix']   

def backupData(sinkSourcesList, container, date):
    for sink in sinkSourcesList:
        if sink['type'] == 'azure':
            backup_dir = 'backup-{}'.format(sink['prefix'])
            copy_data(container, sink['prefix'], backup_dir, date)
            delete_data(container, sink['prefix'], date)

def deleteBackupData(sinkSourcesList, container, date):
    pgDisableSegmentsQuery = "update druid_segments set used='f' where created_date >= '{}' and created_date <= '{}' and datasource = '{}' and used='t'"
    for sink in sinkSourcesList:
        if sink['type'] == 'azure':
            backup_dir = 'backup-{}/{}'.format(sink['prefix'], sink['prefix'])
            delete_data(container, backup_dir, date)
        if sink['type'] == 'druid':
            end_date = date + timedelta(1)
            disableQuery = pgDisableSegmentsQuery.format(date.strftime('%Y-%m-%d %H:%M:%S'), end_date.strftime('%Y-%m-%d %H:%M:%S'), sink['prefix'])
            segments = executeQuery('postgis_test', disableQuery)

def restoreBackupData(sinkSourcesList, container, date):
    for sink in sinkSourcesList:
        if sink['type'] == 'azure':
            backup_dir = 'backup-{}/{}'.format(sink['prefix'], sink['prefix'])
            copy_data(container, backup_dir, sink['prefix'], date)
            delete_data(container, backup_dir, date)           

def getFilterStr(filters):
    filterRes = []
    for filter in filters:
        if filter['value']:
            filterRes.append('{} {} "{}"'.format(filter['key'], filter['operator'], filter['value']))
        else:
            filterRes.append('{} {}'.format(filter['key'], filter['operator']))
    return " and ".join(filterRes)        


