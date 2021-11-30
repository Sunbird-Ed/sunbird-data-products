import os
import sys, time
import findspark
import requests

from datetime import datetime, timedelta
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

from dataproducts.util.utils import create_json, write_data_to_blob, \
                get_data_from_blob, push_metric_event

class ECGLearning:
    def __init__(self, data_store_location, is_bootstrap):
        self.data_store_location = data_store_location
        self.is_bootstrap = is_bootstrap
        self.write_path = ""
        self.csv_file_name = ""
        self.current_time = None
        self.prometheus_host = os.environ['PROMETHEUS_HOST']


    def get_monitoring_data(self, from_time, to_time):
        url = "{}/prometheus/api/v1/query_range".format(self.prometheus_host)
        querystring = {"query": "sum(rate(nginx_http_requests_total{cluster=~\"$cluster\"}[5m]))",
                       "start": str(from_time), "end": str(to_time), "step": "900"}
        headers = {
            'Accept': "application/json, text/plain, */*",
        }
        response = requests.request("GET", url, headers=headers, params=querystring)
        return response.json()["data"]["result"][0]["values"]


    def remove_last_day(self, df):
        current_hour = self.current_time.hour
        if current_hour == 0 and df.count() >= 672:
            first_date = (self.current_time + timedelta(days=-8)).strftime("%Y/%m/%d")
            df = df.filter(~F.col("time").contains(first_date))
        return df


    def generate_reports(self, from_time, to_time):
        ecg_data = self.get_monitoring_data(from_time, to_time)

        findspark.init()
        spark = SparkSession.builder.appName("ECGLearning").master("local[*]").getOrCreate()
        spark.conf.set('spark.sql.session.timeZone', 'Asia/Kolkata')
        os.makedirs(os.path.join(self.write_path, 'public'), exist_ok=True)

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
        get_data_from_blob(Path(self.write_path).joinpath('public', self.csv_file_name))
        current_blob_df = spark.read.csv(os.path.join(self.write_path, 'public', self.csv_file_name), header=True)
        current_blob_df = current_blob_df.withColumn("tps", current_blob_df["tps"].cast("int"))
        current_blob_df = current_blob_df.union(tps_df)
        current_blob_df = current_blob_df.dropDuplicates(["time"])
        current_blob_df = current_blob_df.sort("time")

        # removing the first day's data on 7 days data
        current_blob_df = self.remove_last_day(current_blob_df)

        os.makedirs(os.path.join(self.write_path, 'public'), exist_ok=True)
        current_blob_df.toPandas().to_csv(os.path.join(self.write_path, 'public', self.csv_file_name), index=False)
        create_json(os.path.join(self.write_path, 'public', self.csv_file_name), True)

        # Uploading updated data to Azure blob container
        write_data_to_blob(self.write_path, os.path.join('public', self.csv_file_name))
        write_data_to_blob(self.write_path, os.path.join('public', self.json_file_name))
        
        spark.stop()


    def init(self):
        start_time_sec = int(round(time.time()))
        self.write_path = Path(self.data_store_location).joinpath('ecg_learning_reports')
        self.write_path.mkdir(exist_ok=True)
        print("ECG::Start")
        blob_file_name = "ecg_nation_learning"
        self.csv_file_name = "{}.csv".format(blob_file_name)
        self.json_file_name = "{}.json".format(blob_file_name)
        self.current_time = datetime.now()
        if self.is_bootstrap:
            # For last 7 days
            from_day = (datetime.today() + timedelta(days=-7))
            from_time = int(datetime.combine(from_day, datetime.min.time()).timestamp())
        else:
            # For Last 1 hour
            from_day = (datetime.today() + timedelta(hours=-1))
            from_time = int(from_day.replace(minute=15, second=0, microsecond=0).timestamp())
        to_time = int(self.current_time.timestamp())
        self.generate_reports(from_time, to_time)
        print("ECG::Completed")

        end_time_sec = int(round(time.time()))
        time_taken = end_time_sec - start_time_sec
        metrics = [
            {
                "metric": "timeTakenSecs",
                "value": time_taken
            },
            {
                "metric": "date",
                "value": self.current_time.strftime("%Y-%m-%d %H:%m")
            }
        ]
        push_metric_event(metrics, "ECG Learning")
