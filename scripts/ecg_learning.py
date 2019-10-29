import pdb
import json
import requests
import os
import time
import argparse
import findspark

from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType
from pathlib import Path
from azure.storage.blob import BlockBlobService
from pyspark.sql import functions as F

from utils import create_json, write_data_to_blob, get_data_from_blob

priometheus_host = os.environ['PROMETHEUS_HOST']
account_name = os.environ['AZURE_STORAGE_ACCOUNT_NEW']
account_key = os.environ['AZURE_STORAGE_ACCESS_KEY_NEW']
block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)
findspark.init()

def parsing_necessary_details(from_time, to_time):
    url = "{}/prometheus/api/v1/query_range".format(priometheus_host)
    querystring = {"query": "sum(rate(nginx_request_status_count{cluster=~\"Swarm1|Swarm2\"}[5m]))","start":str(from_time),"end":str(to_time),"step":"900"}
    headers = {
        'Accept': "application/json, text/plain, */*",
    }

    response = requests.request("GET", url, headers=headers, params=querystring)

    return response.json()["data"]["result"][0]["values"]


def remove_last_day(df):
    current_hour = current_time.hour
    if current_hour == 0 & df.count() >= 672:
            first_date = (current_time + timedelta(days=-8)).strftime("%Y/%m/%d")
            df = df.filter(~F.col("time").contains(first_date))

    return df

def init(from_time, to_time):
    ecg_data = parsing_necessary_details(from_time, to_time)

    spark = SparkSession.builder.appName("ECGLearning").master("local[*]").getOrCreate()

    # Create data frame
    ecg_data_rdd = spark.sparkContext.parallelize(ecg_data)
    schema = StructType([
        StructField('time', IntegerType(), True),
        StructField('tps', StringType(), True)
    ])

    tps_df = spark.createDataFrame(ecg_data_rdd,schema)

    tps_df = tps_df.withColumn("tps", tps_df["tps"].cast("float"))
    tps_df = tps_df.withColumn("tps", F.ceil(tps_df["tps"]))
    tps_df = tps_df.withColumn("time", F.from_unixtime(tps_df["time"], "yyyy/MM/dd hh:mm:ss"))

    # Downloading the current file from blob container
    get_data_from_blob('public', csv_file_name)
    current_blob_df = spark.read.csv(os.path.join(write_path, 'public', csv_file_name), header=True)
    current_blob_df = current_blob_df.withColumn("tps", current_blob_df["tps"].cast("int"))

    # removing the first day's data on 7 days data
    current_blob_df = remove_last_day(current_blob_df)

    current_blob_df = current_blob_df.union(tps_df)
    current_blob_df = current_blob_df.dropDuplicates(["time"])
    current_blob_df = current_blob_df.sort("time")

    os.makedirs(os.path.join(write_path, 'public'), exist_ok=True)
    current_blob_df.toPandas().to_csv(os.path.join(write_path, 'public', csv_file_name),index=False)

    create_json(os.path.join(write_path, 'public', csv_file_name), True)

    # # Uploading updated data to Azure blob container
    write_data_to_blob(write_path, os.path.join('public', csv_file_name))
    write_data_to_blob(write_path, os.path.join('public', json_file_name))


parser = argparse.ArgumentParser()
parser.add_argument("--data_store_location", type=str, help="the path to local data folder")
args = parser.parse_args()
write_path = Path(args.data_store_location).joinpath('ecg_learning_reports')

print("ECG::Start")
blob_file_name = "ecg_nation_learning"
csv_file_name = "{}.csv".format(blob_file_name)
json_file_name = "{}.json".format(blob_file_name)
current_time = datetime.now()
from_time = current_time.replace(hour=(current_time.hour-1), minute=15, second=0,microsecond=0).timestamp()
to_time = int(current_time.timestamp())

init(from_time, to_time)
print("ECG::Completed")
