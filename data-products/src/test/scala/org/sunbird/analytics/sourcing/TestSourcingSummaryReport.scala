package org.sunbird.analytics.sourcing

import java.io.File
import java.sql.{Connection, Statement}
import java.time.{ZoneOffset, ZonedDateTime}
import cats.syntax.either._
import ing.wbaa.druid.{DruidConfig, DruidQuery, DruidResponse, DruidResponseTimeseriesImpl, DruidResult, QueryType}
import ing.wbaa.druid.client.DruidClient
import io.circe.Json
import io.circe.parser.parse
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.ekstep.analytics.framework.util.JSONUtils
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers
import org.sunbird.analytics.util.{EmbeddedPostgresql, SparkSpec}

import scala.concurrent.Future

class TestSourcingSummaryReport extends SparkSpec with Matchers with MockFactory {

  implicit var spark: SparkSession = _
  val programTable = "program"
  val nominationTable = "nomination"
  var connection: Connection = null;
  var stmt: Statement = null;

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession()
    EmbeddedPostgresql.start()
    EmbeddedPostgresql.createProgramTable()
    EmbeddedPostgresql.createNominationTable()
    createUserTable()
  }

  override def afterAll() {
    super.afterAll()
    EmbeddedPostgresql.dropTable(programTable)
    EmbeddedPostgresql.dropTable(nominationTable)
    EmbeddedPostgresql.dropTable("\"V_User\"")
    EmbeddedPostgresql.close()
  }

  def createUserTable(): Unit = {
    val tableName: String = "\"V_User\""
    val orgtableName: String = "\"V_User_Org\""
    val userId = "\"userId\""
    val userQuery =
      s"""
         |CREATE TABLE IF NOT EXISTS $tableName (
         |    osid TEXT,
         |    $userId TEXT)""".stripMargin

    val userOrgQuery =
      s"""
         |CREATE TABLE IF NOT EXISTS $orgtableName (
         |    $userId TEXT,
         |    roles TEXT)""".stripMargin

    EmbeddedPostgresql.execute(userQuery)
    EmbeddedPostgresql.execute(userOrgQuery)
  }

  def insertData(): Unit = {
    val tableName: String = "\"V_User\""
    val orgtableName: String = "\"V_User_Org\""
    val userId = "\"userId\""
    val userQuery = s"""INSERT INTO $tableName(osid,$userId) VALUES('0124698765480987654','0124698765480987654')""".stripMargin
    val orgQuery = s"""INSERT INTO $orgtableName($userId, roles) VALUES('0124698765480987654','["admin"]')""".stripMargin
    EmbeddedPostgresql.execute(userQuery)
    EmbeddedPostgresql.execute(orgQuery)
  }

  it should "generate the sourcing report" in {
    implicit val sc = spark.sparkContext
    implicit val mockFc = mock[FrameworkContext]
    val resourceName = "ingestion-spec/sourcing-ingestion-spec.json"
    val classLoader = getClass.getClassLoader
    val file = new File(classLoader.getResource(resourceName).getFile)
    val absolutePath = file.getAbsolutePath
    val config = s"""{"search": {"type": "none"},"model": "org.ekstep.analytics.job.report.SourcingReports","modelParams": {"druidHost":"https://11087f25-9a3f-4305-98b7-9fee523e7967.mock.pstmn.io","specPath":"$absolutePath","dbName":"postgres","druidQuery": {"queryType": "groupBy","dataSource": "vdn-content-model-snapshot","intervals": "1901-01-01T00:00:00+00:00/2101-01-01T00:00:00+00:00","aggregations": [{"name": "count","type": "count"}],"dimensions": [{"fieldName": "primaryCategory","aliasName": "primaryCategory"},{"fieldName": "createdBy","aliasName": "createdBy"}],  "filters": [{"type": "equals","dimension": "objectType","value": "Content"}, {"type": "equals","dimension": "sampleContent","value": "false"}],"postAggregation": [],"descending": "false","limitSpec": {"type": "default","limit": 1000000,"columns": [{"dimension": "count","direction": "descending"}]}},"reportConfig": {"id": "funnel_report","metrics": [],"labels": {"reportDate": "Report generation date","visitors": "No. of users opening the project","projectName": "Project Name","initiatedNominations": "No. of initiated nominations","rejectedNominations": "No. of rejected nominations","pendingNominations": "No. of nominations pending review","acceptedNominations": "No. of accepted nominations to the project","noOfContributors": "No. of contributors to the project","noOfContributions": "No. of contributions to the project","pendingContributions": "No. of contributions pending review","approvedContributions": "No. of approved contributions"},"output": [{"type": "csv","dims": ["identifier", "channel", "name"],"fileParameters": ["id", "dims"]}, {"type": "json","dims": ["identifier", "channel", "name"],"fileParameters": ["id", "dims"]}]},"store": "azure","format": "csv","key": "druid-reports/","filePath": "druid-reports/","container": "reportPostContainer","folderPrefix": ["slug", "reportName"]},"sparkCassandraConnectionHost": "sunbirdPlatformCassandraHost","druidConfig": {"queryType": "timeseries","dataSource": "telemetry-events-syncts","intervals": "startdate/enddate","aggregations": [{"name": "visitors","type": "count","fieldName": "actor_id"}],"filters": [{"type": "equals","dimension": "context_cdata_id","value": "program_id"}, {"type": "equals","dimension": "edata_pageid","value": "contribution_project_contributions"}, {"type": "equals","dimension": "context_pdata_pid","value": "creation-portal.programs"}, {"type": "equals","dimension": "context_cdata_type","value": "project"}, {"type": "equals","dimension": "context_env","value": "creation-portal"}, {"type": "equals","dimension": "eid","value": "IMPRESSION"}],"postAggregation": [],"descending": "false","limitSpec": {"type": "default","limit": 1000000,"columns": [{"dimension": "count","direction": "descending"}]}},"output": [{"to": "console","params": {"printEvent": false}}],"parallelization": 8,"appName": "Funnel Report Job","deviceMapping": false}""".stripMargin
    implicit val jobConfig = JSONUtils.deserialize[JobConfig](config)

    implicit val sqlContext = new SQLContext(sc)

    //mocking for DruidDataFetcher
    import scala.concurrent.ExecutionContext.Implicits.global
    val json: String =
      """
        |{
        |    "primaryCategory": "Practise Question Set",
        |    "createdBy": "0124698765480987654",
        |    "count": 2.0
        |  }
      """.stripMargin

    val doc: Json = parse(json).getOrElse(Json.Null)
    val results = List(DruidResult.apply(Some(ZonedDateTime.of(2020, 1, 23, 17, 10, 3, 0, ZoneOffset.UTC)), doc))
    val druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.GroupBy)

    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.doQuery[DruidResponse](_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future(druidResponse)).anyNumberOfTimes()
    (mockFc.getDruidRollUpClient _).expects().returns(mockDruidClient).anyNumberOfTimes()

    SourcingSummaryReport.execute()

    val report = sqlContext.sparkSession.read
      .option("header", "false")
      .json("sourcing/SourcingSummaryReport.json")

    report.first().getString(0) should be("0124698765480987654")
    report.first().getString(1) should be("Practise Question Set")
    report.first().getLong(3) should be(2L)
    report.first().getString(4) should be("Individual")
  }

  it should "get correct userType for users" in {
    implicit val sc = spark.sparkContext
    implicit val mockFc = mock[FrameworkContext]
    val config = s"""{"search": {"type": "none"},"model": "org.ekstep.analytics.job.report.SourcingReports","modelParams": {"tables": {"programTable": "program", "nominationTable": "nomination"},"druidIngestionUrl":"https://httpbin.org/post","specPath":"absolutePath","dbName":"postgres","druidQuery": {"queryType": "groupBy","dataSource": "vdn-content-model-snapshot","intervals": "1901-01-01T00:00:00+00:00/2101-01-01T00:00:00+00:00","aggregations": [{"name": "count","type": "count"}],"dimensions": [{"fieldName": "primaryCategory","aliasName": "primaryCategory"},{"fieldName": "createdBy","aliasName": "createdBy"}],  "filters": [{"type": "equals","dimension": "objectType","value": "Content"}, {"type": "equals","dimension": "sampleContent","value": "false"}],"postAggregation": [],"descending": "false","limitSpec": {"type": "default","limit": 1000000,"columns": [{"dimension": "count","direction": "descending"}]}},"reportConfig": {"id": "funnel_report","metrics": [],"labels": {"reportDate": "Report generation date","visitors": "No. of users opening the project","projectName": "Project Name","initiatedNominations": "No. of initiated nominations","rejectedNominations": "No. of rejected nominations","pendingNominations": "No. of nominations pending review","acceptedNominations": "No. of accepted nominations to the project","noOfContributors": "No. of contributors to the project","noOfContributions": "No. of contributions to the project","pendingContributions": "No. of contributions pending review","approvedContributions": "No. of approved contributions"},"output": [{"type": "csv","dims": ["identifier", "channel", "name"],"fileParameters": ["id", "dims"]}, {"type": "json","dims": ["identifier", "channel", "name"],"fileParameters": ["id", "dims"]}]},"store": "azure","format": "csv","key": "druid-reports/","filePath": "druid-reports/","container": "reportPostContainer","folderPrefix": ["slug", "reportName"]},"sparkCassandraConnectionHost": "sunbirdPlatformCassandraHost","druidConfig": {"queryType": "timeseries","dataSource": "telemetry-events-syncts","intervals": "startdate/enddate","aggregations": [{"name": "visitors","type": "count","fieldName": "actor_id"}],"filters": [{"type": "equals","dimension": "context_cdata_id","value": "program_id"}, {"type": "equals","dimension": "edata_pageid","value": "contribution_project_contributions"}, {"type": "equals","dimension": "context_pdata_pid","value": "creation-portal.programs"}, {"type": "equals","dimension": "context_cdata_type","value": "project"}, {"type": "equals","dimension": "context_env","value": "creation-portal"}, {"type": "equals","dimension": "eid","value": "IMPRESSION"}],"postAggregation": [],"descending": "false","limitSpec": {"type": "default","limit": 1000000,"columns": [{"dimension": "count","direction": "descending"}]}},"output": [{"to": "console","params": {"printEvent": false}}],"parallelization": 8,"appName": "Funnel Report Job","deviceMapping": false}""".stripMargin
    implicit val jobConfig = JSONUtils.deserialize[JobConfig](config)
    insertData()
    //mocking for DruidDataFetcher
    import scala.concurrent.ExecutionContext.Implicits.global
    val json: String =
      """
        |{
        |    "primaryCategory": "Practise Question Set",
        |    "createdBy": "0124698765480987654",
        |    "count": 2.0
        |  }
      """.stripMargin

    val doc: Json = parse(json).getOrElse(Json.Null)
    val results = List(DruidResult.apply(Some(ZonedDateTime.of(2020, 1, 23, 17, 10, 3, 0, ZoneOffset.UTC)), doc))
    val druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.GroupBy)

    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.doQuery[DruidResponse](_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future(druidResponse)).anyNumberOfTimes()
    (mockFc.getDruidRollUpClient _).expects().returns(mockDruidClient).anyNumberOfTimes()

    val userDf = SourcingSummaryReport.getUserDetails()
    userDf.select("osid").first().getString(0) should be("0124698765480987654")
    userDf.select("user_type").first().getString(0) should be("Organization")
  }

}
