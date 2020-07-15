#!/usr/bin/env bash

home=`echo $HOME`
jobJarPath="/Users/manju/Documents/Ekstep/Github/sunbird-data-pipeline/druid/etl-jobs/target/etl-jobs-1.0.jar"
jobConfPath="${home}/etl-jobs-1.0/resources/cassandraRedis.conf"

# /Users/manju/Documents/Ekstep/Softwares/spark-2.4.4-bin-hadoop2.7/bin/spark-submit \
# --class org.ekstep.analytics.jobs.CassandraRedisIndexer  \
# ${jobJarPath}

/Users/manju/Documents/Ekstep/Softwares/spark-2.4.4-bin-hadoop2.7/bin/spark-submit/bin/spark-submit --master local[*] --jars /Users/manju/Documents/Ekstep/Github/sunbird-data-pipeline/druid/etl-jobs/target/etl-jobs-1.0.jar,  --class org.ekstep.analytics.jobs.CassandraRedisIndexer

