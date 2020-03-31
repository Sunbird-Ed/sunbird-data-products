import findspark as fs
import os
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import json

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0,org.postgresql:postgresql:9.4.1211 --conf spark.cassandra.connection.host=localhost pyspark-shell'
#cassandra_keyspace_prefix = os.environ['ENV']
#host_name = os.environ['POSTGRES_HOSTNAME']
#db_name = os.environ['POSTGRES_DBNAME']
#user_name = os.environ['POSTGRES_USERNAME']
#password = os.environ['POSTGRES_PASSWORD']
#url = "jdbc:postgresql://{}/{}".format(host_name, db_name)
cassandra_keyspace_prefix = "local"
user_name = "<postgres-user-name>"
password = "<postgres-password>"
url = "jdbc:postgresql://localhost:5432/<postgres-db>"
connProperties = {
    "driver": "org.postgresql.Driver",
    "user": user_name,
    "password": password,
    "stringtype": "unspecified"
}
fs.init()

deviceKeyspace = "{}_device_db".format(cassandra_keyspace_prefix)
spark = SparkSession.builder.appName("DailyscansVerification").master("local[*]").getOrCreate()
cassandra_df = spark.read\
        .format("org.apache.spark.sql.cassandra")\
        .options(table="device_profile", keyspace=deviceKeyspace)\
        .load()

cass_count = cassandra_df.count()
print('Cassandra data: {}'.format(cass_count))

postgres_df = spark.read\
        .format("jdbc")\
        .option("url", url)\
        .option("dbtable", "device_profile")\
        .option("user", user_name)\
        .option("password", password)\
        .option("driver", "org.postgresql.Driver")\
        .load()

post_count = postgres_df.count()
print('Postgres data: {}'.format(post_count))

df = cassandra_df.join(postgres_df, on=['device_id'], how='left_anti')
missing_count = df.count()
print('Postgres missing data count: {}'.format(missing_count))
#df.show()
final_df = df.withColumn("uaspec", F.to_json(df["uaspec"])).withColumn("device_spec", F.to_json(df["device_spec"]))
#final_df = df.drop(df["uaspec"]).drop(df["device_spec"])
#final_df.show()
#final_df.printSchema()

final_df.write.jdbc(url, "device_profile", "append", connProperties)

latest_postgres_df = spark.read\
        .format("jdbc")\
        .option("url", url)\
        .option("dbtable", "device_profile")\
        .option("user", user_name)\
        .option("password", password)\
        .option("driver", "org.postgresql.Driver")\
        .load()

latest_post_count = latest_postgres_df.count()
print('Postgres data count after migration: {}'.format(latest_post_count))
