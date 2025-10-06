package org.sunbird.analytics.job.report

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import org.ekstep.analytics.framework.util.{HadoopFileUtil, JSONUtils}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.ekstep.analytics.framework.conf.AppConf
import org.scalamock.scalatest.MockFactory

class TestUserMasterDataIndexerV2 extends BaseReportSpec with MockFactory {

  var spark: SparkSession = _
  var userDF: DataFrame = _
  var reporterMock: BaseReportsJob = mock[BaseReportsJob]

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession()
    userDF = spark.read.json("src/test/resources/collection-summary/user_data.json").cache()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    val objectKey = AppConf.getConfig("course.metrics.cloud.objectKey")
    new HadoopFileUtil().delete(spark.sparkContext.hadoopConfiguration, objectKey + "user-exhaust/")
  }

  it should "generate user exhaust from user cache and save to blob" in {
    initializeMockData()
    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.UserMasterDataIndexerV2","modelParams":{"store":"azure","sparkElasticsearchConnectionHost":"localhost","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"12","sparkCassandraConnectionHost":"localhost","reportPath":"user-exhaust/"},"parallelization":2,"appName":"User Exhaust Report"}"""
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](strConfig)

    val reportData = UserMasterDataIndexerV2.prepareReport(spark, reporterMock.fetchData _)
    assert(reportData.count() > 0)
  }

  def initializeMockData(): Unit = {
    val schema = Encoders.product[org.sunbird.analytics.job.report.UserCols].schema
    (reporterMock.fetchData _)
      .expects(spark, Map("table" -> "user", "infer.schema" -> "true", "key.column" -> "userid"), "org.apache.spark.sql.redis", schema)
      .returning(userDF)
      .anyNumberOfTimes()
  }
}


