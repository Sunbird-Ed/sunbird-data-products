package org.sunbird.analytics.exhaust

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.ekstep.analytics.framework.util.JSONUtils
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.exhaust.collection.{ResponseExhaustJob, UserData}
import org.sunbird.analytics.job.report.BaseReportSpec
import org.sunbird.analytics.util.EmbeddedCassandra

class TestResponseExhaustJob extends BaseReportSpec with MockFactory with BaseReportsJob {

  var reportMock: BaseReportsJob = mock[BaseReportsJob]
  var spark: SparkSession = _
  var userDF: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession();

    userDF = spark.read.json("src/test/resources/exhaust/user_data.json").cache()

    EmbeddedCassandra.loadData("src/test/resources/exhaust/report_data.cql") // Load test data in embedded cassandra server
  }

  override def afterAll() : Unit = {
    super.afterAll();
  }

  "TestResponseExxhaustJob" should "generate final output as csv and zip files" in {
    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.ProgressExhaustJob","modelParams":{"store":"azure","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"localhost","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"12","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":""},"parallelization":8,"appName":"Progress Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig
    implicit val sparkSession: SparkSession = spark

    val schema = Encoders.product[UserData].schema

    (reportMock.loadData(_: Map[String, String], _: String, _: StructType )(_: SparkSession))
      .expects(Map("table" -> "user","infer.schema" -> "true", "key.column"-> "userid"),"org.apache.spark.sql.redis", schema, *)
      .anyNumberOfTimes()
      .returning(userDF)

    val jobResponse = ResponseExhaustJob.execute()
  }

}
