package org.sunbird.analytics.updater

import org.apache.spark.SparkContext
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.util.BaseSpec

class TestCassandraMigratorJob extends BaseSpec with MockFactory {
  implicit  var sc: SparkContext = _
  val config ="{\"search\":{\"type\":\"none\"},\"model\":\"org.sunbird.analytics.updater.CassandraMigratorJob\",\"modelParams\":{\"cassandraDataHost\":\"127.0.0.1\",\"cassandraDataPort\":\"9142\",\"cassandraMigrateHost\":\"127.0.0.1\",\"cassandraMigratePort\":\"9142\",\"keyspace\":\"test_keyspace\",\"table\":\"user_enrolment_test\"},\"output\":[{\"to\":\"console\",\"params\":{\"printEvent\":false}}],\"parallelization\":10,\"appName\":\"Cassandra Migrator\",\"deviceMapping\":false}"
  override def beforeAll: Unit = {
    super.beforeAll()

  }
  override def afterAll() {
    super.afterAll()

  }

  "CassandraMigratorJob" should "Migrate cassandra data" in {
    CassandraMigratorJob.main(config)(Option(sc))
  }

  "CassandraMigratorJob" should "Migrate cassandra data with repartition" in {
    val config ="{\"search\":{\"type\":\"none\"},\"model\":\"org.sunbird.analytics.updater.CassandraMigratorJob\",\"modelParams\":{\"cassandraDataHost\":\"127.0.0.1\",\"cassandraDataPort\":\"9142\",\"cassandraMigrateHost\":\"127.0.0.1\",\"cassandraMigratePort\":\"9142\",\"keyspace\":\"test_keyspace\",\"table\":\"user_enrolment_test\",\"repartitionColumns\":\"batchid\"},\"output\":[{\"to\":\"console\",\"params\":{\"printEvent\":false}}],\"parallelization\":10,\"appName\":\"Cassandra Migrator\",\"deviceMapping\":false}"
    CassandraMigratorJob.main(config)(Option(sc))

  }
}
