import os
import sys, time
from datetime import datetime, timedelta
from pathlib import Path

import argparse
import findspark
import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

util_path = os.path.abspath(os.path.join(__file__, '..', '..', '..', 'util'))
sys.path.append(util_path)

from utils import create_json, write_data_to_blob, get_data_from_blob, push_metric_event

prometheus_host = os.environ['PROMETHEUS_HOST']
findspark.init()


def get_monitoring_data(from_time, to_time):
    url = "{}/prometheus/api/v1/query_range".format(prometheus_host)
    querystring = {"query": "sum(rate(nginx_request_status_count{cluster=~\"Swarm1|Swarm2\"}[5m]))",
                   "start": str(from_time), "end": str(to_time), "step": "900"}
    headers = {
        'Accept': "application/json, text/plain, */*",
    }
    response = requests.request("GET", url, headers=headers, params=querystring)
    return response.json()["data"]["result"][0]["values"]


def remove_last_day(df):
    current_hour = current_time.hour
    if current_hour == 0 and df.count() >= 672:
        first_date = (current_time + timedelta(days=-8)).strftime("%Y/%m/%d")
        df = df.filter(~F.col("time").contains(first_date))
    return df


def init(from_time, to_time):
    ecg_data = get_monitoring_data(from_time, to_time)
    spark = SparkSession.builder.appName("ECGLearning").master("local[*]").getOrCreate()
    spark.conf.set('spark.sql.session.timeZone', 'Asia/Kolkata')
    os.makedirs(os.path.join(write_path, 'public'), exist_ok=True)
    # Create data frame
    ecg_data_rdd = spark.sparkContext.parallelize(ecg_data)
    schema = StructType([
        StructField('time', IntegerType(), True),
        StructField('tps', StringType(), True)
    ])
    tps_df = spark.createDataFrame(ecg_data_rdd, schema)
    tps_df = tps_df.withColumn("tps", tps_df["tps"].cast("float"))
    tps_df = tps_df.withColumn("tps", F.ceil(tps_df["tps"]))
    tps_df = tps_df.withColumn("time", F.from_unixtime(tps_df["time"], "yyyy/MM/dd HH:mm:ss"))
    # Downloading the current file from blob container
    get_data_from_blob(Path(write_path).joinpath('public', csv_file_name))
    current_blob_df = spark.read.csv(os.path.join(write_path, 'public', csv_file_name), header=True)
    current_blob_df = current_blob_df.withColumn("tps", current_blob_df["tps"].cast("int"))
    current_blob_df = current_blob_df.union(tps_df)
    current_blob_df = current_blob_df.dropDuplicates(["time"])
    current_blob_df = current_blob_df.sort("time")
    # removing the first day's data on 7 days data
    current_blob_df = remove_last_day(current_blob_df)
    os.makedirs(os.path.join(write_path, 'public'), exist_ok=True)
    current_blob_df.toPandas().to_csv(os.path.join(write_path, 'public', csv_file_name), index=False)
    create_json(os.path.join(write_path, 'public', csv_file_name), True)
    # # Uploading updated data to Azure blob container
    write_data_to_blob(write_path, os.path.join('public', csv_file_name))
    write_data_to_blob(write_path, os.path.join('public', json_file_name))

start_time_sec = int(round(time.time()))
parser = argparse.ArgumentParser()
parser.add_argument("--data_store_location", type=str, help="the path to local data folder")
parser.add_argument("--bootstrap", type=str, help="run for 7 days")
args = parser.parse_args()
is_bootstrap = args.bootstrap
write_path = Path(args.data_store_location).joinpath('ecg_learning_reports')
write_path.mkdir(exist_ok=True)
print("ECG::Start")
blob_file_name = "ecg_nation_learning"
csv_file_name = "{}.csv".format(blob_file_name)
json_file_name = "{}.json".format(blob_file_name)
current_time = datetime.now()
if is_bootstrap == "true":
    # For last 7 days
    from_day = (datetime.today() + timedelta(days=-7))
    from_time = int(datetime.combine(from_day, datetime.min.time()).timestamp())
else:
    # For Last 1 hour
    from_day = (datetime.today() + timedelta(hours=-1))
    from_time = int(from_day.replace(minute=15, second=0, microsecond=0).timestamp())
to_time = int(current_time.timestamp())
init(from_time, to_time)
print("ECG::Completed")

end_time_sec = int(round(time.time()))
time_taken = end_time_sec - start_time_sec
metrics = {
    "system": "AdhocJob",
    "subsystem": "ECG Learning",
    "metrics": [
        {
            "metric": "timeTakenSecs",
            "value": time_taken
        },
        {
            "metric": "date",
            "value": datetime.strptime(current_time, "%Y-%m-%d")
        }
    ]
}
push_metric_event(metrics, "ECG Learning")