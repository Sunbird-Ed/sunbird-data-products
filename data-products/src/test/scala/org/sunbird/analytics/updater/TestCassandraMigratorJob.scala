package org.sunbird.analytics.updater

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.util.JSONUtils
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.util.{BaseSpec, SparkSpec}

class TestCassandraMigratorJob extends SparkSpec(file =null) with MockFactory {

  val config ="{\"search\":{\"type\":\"none\"},\"model\":\"org.sunbird.analytics.updater.CassandraMigratorJob\",\"modelParams\":{\"cassandraDataHost\":\"127.0.0.1\",\"cassandraDataPort\":\"9142\",\"cassandraMigrateHost\":\"127.0.0.1\",\"cassandraMigratePort\":\"9142\",\"keyspace\":\"test_keyspace\",\"table\":\"user_enrolment_test\"},\"output\":[{\"to\":\"console\",\"params\":{\"printEvent\":false}}],\"parallelization\":10,\"appName\":\"Cassandra Migrator\",\"deviceMapping\":false}"
  "CassandraMigratorJob" should "Migrate cassandra data" in {
    CassandraMigratorJob.main(config)(Option(sc))
  }

  it should "Migrate cassandra data with data repartition" in {
    val config ="{\"search\":{\"type\":\"none\"},\"model\":\"org.sunbird.analytics.updater.CassandraMigratorJob\",\"modelParams\":{\"cassandraDataHost\":\"127.0.0.1\",\"cassandraDataPort\":\"9142\",\"cassandraMigrateHost\":\"127.0.0.1\",\"cassandraMigratePort\":\"9142\",\"keyspace\":\"test_keyspace\",\"table\":\"user_enrolment_test\",\"dataRepartitionColumns\":\"userid\",\"repartitionColumns\":\"batchid\"},\"output\":[{\"to\":\"console\",\"params\":{\"printEvent\":false}}],\"parallelization\":10,\"appName\":\"Cassandra Migrator\",\"deviceMapping\":false}"
    val jobConfig = JSONUtils.deserialize[JobConfig](config)
    CassandraMigratorJob.migrateData(jobConfig)(getSparkContext())

  }

  it should "Migrate cassandra data with migrate repartition" in {
    val config ="{\"search\":{\"type\":\"none\"},\"model\":\"org.sunbird.analytics.updater.CassandraMigratorJob\",\"modelParams\":{\"cassandraDataHost\":\"127.0.0.1\",\"cassandraDataPort\":\"9142\",\"cassandraMigrateHost\":\"127.0.0.1\",\"cassandraMigratePort\":\"9142\",\"keyspace\":\"test_keyspace\",\"table\":\"user_enrolment_test\",\"repartitionColumns\":\"batchid\"},\"output\":[{\"to\":\"console\",\"params\":{\"printEvent\":false}}],\"parallelization\":10,\"appName\":\"Cassandra Migrator\",\"deviceMapping\":false}"
    val jobConfig = JSONUtils.deserialize[JobConfig](config)
    CassandraMigratorJob.migrateData(jobConfig)(getSparkContext())

  }
}
