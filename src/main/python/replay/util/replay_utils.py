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
from azure_utils import copy_data, delete_data, get_data_path
from postgres_utils import executeQuery
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError

findspark.init()

def push_data(broker_host, topic, container, prefix, date):
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
    def push_data_kafka(events):
        kafka_producer = KafkaProducer(bootstrap_servers=[broker_host])
        for event in events:
            kafka_producer.send(topic, bytearray(event, 'utf-8'))
            kafka_producer.flush()
    df.toJSON().foreachPartition(push_data_kafka)
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

def getKafkaTopic(config_json, prefix):
    return config_json[prefix]['outputKafkaTopic']