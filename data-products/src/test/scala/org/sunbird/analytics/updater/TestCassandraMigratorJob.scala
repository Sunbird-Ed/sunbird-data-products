package org.sunbird.analytics.updater

import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.util.JSONUtils
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.util.SparkSpec

class TestCassandraMigratorJob extends SparkSpec(file =null) with MockFactory {

  val config ="{\"search\":{\"type\":\"none\"},\"model\":\"org.sunbird.analytics.updater.CassandraMigratorJob\",\"modelParams\":{\"cassandraDataHost\":\"127.0.0.1\",\"cassandraDataPort\":\"9142\",\"cassandraMigrateHost\":\"127.0.0.1\",\"cassandraMigratePort\":\"9142\",\"keyspace\":\"test_keyspace\",\"cassandraDataTable\":\"user_enrolment_test\",\"cassandraMigrateTable\":\"report_user_enrolments\"},\"output\":[{\"to\":\"console\",\"params\":{\"printEvent\":false}}],\"parallelization\":10,\"appName\":\"Cassandra Migrator\",\"deviceMapping\":false}"
  "CassandraMigratorJob" should "Migrate cassandra data" in {
    CassandraMigratorJob.main(config)(Option(sc))
  }

  it should "Migrate cassandra data with data repartition" in {
    val config ="{\"search\":{\"type\":\"none\"},\"model\":\"org.sunbird.analytics.updater.CassandraMigratorJob\",\"modelParams\":{\"cassandraDataHost\":\"127.0.0.1\",\"cassandraDataPort\":\"9142\",\"cassandraMigrateHost\":\"127.0.0.1\",\"cassandraMigratePort\":\"9142\",\"keyspace\":\"test_keyspace\",\"cassandraDataTable\":\"user_enrolment_test\",\"cassandraMigrateTable\":\"report_user_enrolments\",\"dataRepartitionColumns\":\"userid\",\"repartitionColumns\":\"batchid\"},\"output\":[{\"to\":\"console\",\"params\":{\"printEvent\":false}}],\"parallelization\":10,\"appName\":\"Cassandra Migrator\",\"deviceMapping\":false}"
    val jobConfig = JSONUtils.deserialize[JobConfig](config)
    val dataCount = CassandraMigratorJob.migrateData(jobConfig)(getSparkContext())
    dataCount should be(1)
  }

  it should "Migrate cassandra data with migrate repartition" in {
    val config ="{\"search\":{\"type\":\"none\"},\"model\":\"org.sunbird.analytics.updater.CassandraMigratorJob\",\"modelParams\":{\"cassandraDataHost\":\"127.0.0.1\",\"cassandraDataPort\":\"9142\",\"cassandraMigrateHost\":\"127.0.0.1\",\"cassandraMigratePort\":\"9142\",\"keyspace\":\"test_keyspace\",\"cassandraDataTable\":\"user_enrolment_test\",\"cassandraMigrateTable\":\"report_user_enrolments\",\"repartitionColumns\":\"batchid\"},\"output\":[{\"to\":\"console\",\"params\":{\"printEvent\":false}}],\"parallelization\":10,\"appName\":\"Cassandra Migrator\",\"deviceMapping\":false}"
    val jobConfig = JSONUtils.deserialize[JobConfig](config)
    CassandraMigratorJob.migrateData(jobConfig)(getSparkContext())

  }

  it should "Migrate cassandra data with different migrate table in same cluster" in {
    val modelConfig ="{\"search\":{\"type\":\"none\"},\"model\":\"org.sunbird.analytics.updater.CassandraMigratorJob\",\"modelParams\":{\"cassandraDataHost\":\"127.0.0.1\",\"cassandraDataPort\":\"9142\",\"cassandraMigrateHost\":\"127.0.0.1\",\"cassandraMigratePort\":\"9142\",\"keyspace\":\"test_keyspace\",\"cassandraDataTable\":\"user_enrolment_test\",\"cassandraMigrateTable\":\"report_user_enrolments\",\"repartitionColumns\":\"batchid\"},\"output\":[{\"to\":\"console\",\"params\":{\"printEvent\":false}}],\"parallelization\":10,\"appName\":\"Cassandra Migrator\",\"deviceMapping\":false}"
    val jobConfig = JSONUtils.deserialize[JobConfig](modelConfig)
    CassandraMigratorJob.migrateData(jobConfig)(getSparkContext())
  }

  it should "Test for exception case when cassandra is not running" in {

    val config ="{\"search\":{\"type\":\"none\"},\"model\":\"org.sunbird.analytics.updater.CassandraMigratorJob\",\"modelParams\":{\"cassandraDataHost\":\"127.0.0.2\",\"cassandraDataPort\":\"9142\",\"cassandraMigrateHost\":\"127.0.0.2\",\"cassandraMigratePort\":\"9142\",\"keyspace\":\"test_keyspace\",\"cassandraDataTable\":\"user_enrolment_test\",\"cassandraMigrateTable\":\"report_user_enrolments\"},\"output\":[{\"to\":\"console\",\"params\":{\"printEvent\":false}}],\"parallelization\":10,\"appName\":\"Cassandra Migrator\",\"deviceMapping\":false}"
    CassandraMigratorJob.main(config)(Option(sc))
  }
}
