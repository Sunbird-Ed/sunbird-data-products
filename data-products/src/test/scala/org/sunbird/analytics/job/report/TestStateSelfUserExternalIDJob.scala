package org.sunbird.analytics.job.report

import java.io.File

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions.col
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.util.HadoopFileUtil
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.util.{DecryptUtil, EmbeddedCassandra}
import org.sunbird.cloud.storage.conf.AppConf

class TestStateSelfUserExternalIDJob extends BaseReportSpec with MockFactory {

  implicit var spark: SparkSession = _
  var map: Map[String, String] = _
  var shadowUserDF: DataFrame = _
  var orgDF: DataFrame = _
  var reporterMock: BaseReportsJob = mock[BaseReportsJob]
  val sunbirdKeyspace = "sunbird"
  val shadowUserEncoder = Encoders.product[ShadowUserData].schema

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession();
    EmbeddedCassandra.loadData("src/test/resources/reports/user_self_test_data.cql") // Load test data in embedded cassandra server
  }
  
  override def afterAll() : Unit = {
    super.afterAll();
    (new HadoopFileUtil()).delete(spark.sparkContext.hadoopConfiguration, "src/test/resources/admin-user-reports")
  }

  //Created data : channels ApSlug and OtherSlug contains validated users created against blocks,districts and state
  //Only TnSlug doesn't contain any validated users
  "StateSelfUserExternalID" should "generate reports" in {
    implicit val fc = new FrameworkContext()
    val tempDir = AppConf.getConfig("admin.metrics.temp.dir")
    DecryptUtil.initialise()
    val reportDF = StateAdminReportJob.generateExternalIdReport()(spark, fc)
    assert(reportDF.columns.contains("Diksha UUID") === true)
    assert(reportDF.columns.contains("Name") === true)
    assert(reportDF.columns.contains("State") === true)
    assert(reportDF.columns.contains("District") === true)
    assert(reportDF.columns.contains("School Name") === true)
    assert(reportDF.columns.contains("School UDISE ID") === true)
    assert(reportDF.columns.contains("State provided ext. ID") === true)
    assert(reportDF.columns.contains("Phone number") === true)
    assert(reportDF.columns.contains("Email ID") === true)
    val userName = reportDF.select("Name").collect().map(_ (0)).toList
    assert(userName(0) === "localuser115f localuser115l")
  }
}