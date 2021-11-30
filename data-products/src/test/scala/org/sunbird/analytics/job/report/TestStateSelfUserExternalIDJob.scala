package org.sunbird.analytics.job.report

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.ekstep.analytics.framework.util.{HadoopFileUtil, JSONUtils}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.util.{DecryptUtil, EmbeddedCassandra}
import org.sunbird.cloud.storage.conf.AppConf

class TestStateSelfUserExternalIDJob extends BaseReportSpec with MockFactory {

  implicit var spark: SparkSession = _
  var map: Map[String, String] = _
  var orgDF: DataFrame = _
  var reporterMock: BaseReportsJob = mock[BaseReportsJob]
  val sunbirdKeyspace = "sunbird"

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
    val reportDF = StateAdminReportJob.generateExternalIdReport()(spark, fc)
    assert(reportDF.count() === (2));
    assert(reportDF.columns.contains("Diksha UUID") === true)
    assert(reportDF.columns.contains("Name") === true)
    assert(reportDF.columns.contains("State") === true)
    assert(reportDF.columns.contains("District") === true)
    assert(reportDF.columns.contains("Block") === true)
    assert(reportDF.columns.contains("Cluster") === true)
    assert(reportDF.columns.contains("School Name") === true)
    assert(reportDF.columns.contains("School UDISE ID") === true)
    assert(reportDF.columns.contains("State provided ext. ID") === true)
    assert(reportDF.columns.contains("Org Phone") === true)
    assert(reportDF.columns.contains("Org Email ID") === true)
    assert(reportDF.columns.contains("User Type") === true)
    assert(reportDF.columns.contains("User-Sub Type") === true)
    assert(reportDF.columns.contains("Profile Phone number") === true)
    assert(reportDF.columns.contains("Profile Email") === true)
    assert(reportDF.columns.contains("Diksha Sub-Org ID") === false)
    val userName = reportDF.select("Name").collect().map(_ (0)).toList
    assert(userName(0) === "localuser118f localuser118l")
  }
  
  "StateSelfUserExternalIDWithZip" should "execute with zip failed to generate" in {
    implicit val fc = new FrameworkContext()
    try {
      val reportDF = StateAdminReportJob.generateExternalIdReport()(spark, fc)
      StateAdminReportJob.generateSelfUserDeclaredZip(reportDF, JSONUtils.deserialize[JobConfig]("""{"model":"Test"}"""))
    } catch {
      case ex: Exception => assert(ex.getMessage === "Self-Declared user level zip generation failed with exit code 127");
    }
  }
}
