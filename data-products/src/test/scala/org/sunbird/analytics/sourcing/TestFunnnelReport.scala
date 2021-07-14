package org.sunbird.analytics.sourcing

import java.time.{ZoneOffset, ZonedDateTime}

import cats.syntax.either._
import ing.wbaa.druid.client.DruidClient
import ing.wbaa.druid._
import io.circe.Json
import io.circe.parser.parse
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.framework.util.{HadoopFileUtil, JSONUtils}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.ekstep.analytics.framework.util.JSONUtils
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers
import org.sunbird.analytics.util.{EmbeddedPostgresql, SparkSpec}

import scala.concurrent.Future

class TestFunnnelReport extends SparkSpec with Matchers with MockFactory {
  var spark: SparkSession = _
  val programTable = "program"
  val nominationTable = "nomination"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession()
    EmbeddedPostgresql.start()
    EmbeddedPostgresql.createProgramTable()
    EmbeddedPostgresql.createNominationTable()
  }

  override def afterAll() {
    super.afterAll()
    new HadoopFileUtil().delete(spark.sparkContext.hadoopConfiguration, "sourcing")
    EmbeddedPostgresql.dropTable(programTable)
    EmbeddedPostgresql.dropTable(nominationTable)
    EmbeddedPostgresql.close()
  }

  it should "execute and generate Funnel Report" in {
    implicit val sc = spark.sparkContext
    implicit val mockFc = mock[FrameworkContext]

    //Inserting data into postgres
    EmbeddedPostgresql.execute("INSERT INTO program(program_id,status,startdate,enddate,name) VALUES('2bf17140-8124-11e9-bafa-676cba786201','Live','2020-01-10','2025-05-09','Dock-Project')")
    EmbeddedPostgresql.execute("INSERT INTO program(program_id,status,startdate,enddate,name) VALUES('f624il0r-8124-11e9-bafa-676cba755409','Live','2026-01-10','2025-05-09','Dock-Project')")
    EmbeddedPostgresql.execute("INSERT INTO nomination(program_id,status) VALUES('2bf17140-8124-11e9-bafa-676cba786201','Initiated')")

    val config = """{"search": {"type": "none"},"model": "org.ekstep.analytics.job.report.FunnelReport","modelParams": {"contributionConfig": {    "contentRequest": {     "request": {      "filters": {       "programId": "programIdentifier",       "objectType": "content",       "status": ["Draft", "Live", "Review"],       "mimeType": "application/vnd.ekstep.content-collection"      },      "fields": ["acceptedContents", "rejectedContents"],      "limit": 10000     }    },    "correctionsPendingRequest": {     "request": {      "filters": {       "objectType": "content",       "status": "Draft",       "prevStatus": "Live",       "programId": "programIdentifier",       "mimeType": {        "!=": "application/vnd.ekstep.content-collection"       },       "contentType": {        "!=": "Asset"       }      },      "not_exists": ["sampleContent"],      "facets": ["createdBy"],      "limit": 0     }    },    "contributionRequest": {     "request": {      "filters": {       "objectType": ["content","questionset"],       "status": ["Live"],       "programId": "programIdentifier",       "mimeType": {        "!=": "application/vnd.ekstep.content-collection"       },       "contentType": {        "!=": "Asset"       }      },      "not_exists": ["sampleContent"],      "facets": ["createdBy"],      "limit": 0     }    }   },"reportConfig": {"id": "funnel_report","metrics": [],"labels": {"reportDate": "Report generation date","visitors": "No. of users opening the project","projectName": "Project Name","initiatedNominations": "No. of initiated nominations","rejectedNominations": "No. of rejected nominations","pendingNominations": "No. of nominations pending review","acceptedNominations": "No. of accepted nominations to the project","noOfContributors": "No. of contributors to the project","noOfContributions": "No. of contributions to the project","pendingContributions": "No. of contributions pending review","approvedContributions": "No. of approved contributions"},"output": [{"type": "csv","dims": ["identifier", "channel", "name"],"fileParameters": ["id", "dims"]}, {"type": "json","dims": ["identifier", "channel", "name"],"fileParameters": ["id", "dims"]}]},"store": "azure","format": "csv","key": "druid-reports/","filePath": "druid-reports/","container": "'$reportPostContainer'","folderPrefix": ["slug", "reportName"]},"sparkCassandraConnectionHost": "'$sunbirdPlatformCassandraHost'","druidConfig": {"queryType": "timeseries","dataSource": "telemetry-events-syncts","intervals": "startdate/enddate","aggregations": [{"name": "visitors","type": "count","fieldName": "actor_id"}],"filters": [{"type": "equals","dimension": "context_cdata_id","value": "program_id"}, {"type": "equals","dimension": "edata_pageid","value": "contribution_project_contributions"}, {"type": "equals","dimension": "context_pdata_pid","value": "creation-portal.programs"}, {"type": "equals","dimension": "context_cdata_type","value": "project"}, {"type": "equals","dimension": "context_env","value": "creation-portal"}, {"type": "equals","dimension": "eid","value": "IMPRESSION"}],"postAggregation": [],"descending": "false","limitSpec": {"type": "default","limit": 1000000,"columns": [{"dimension": "count","direction": "descending"}]}},"output": [{"to": "console","params": {"printEvent": false}}],"parallelization": 8,"appName": "Funnel Report Job","deviceMapping": false}""".stripMargin
    val configMap = JSONUtils.deserialize[Map[String,AnyRef]](config)
    implicit val jobConfig = JSONUtils.deserialize[JobConfig](config)
    implicit val sparkCon = spark
    val reportMetrics = FunnelReport.execute()
    reportMetrics.getOrElse("funnelReportCount",0) should be (2)
  }

  it should "return 0 if no resources found for correction pending" in {
    val programId = "do_07bc98d57c59"
    val config = """{"search": {"type": "none"},"model": "org.ekstep.analytics.job.report.FunnelReport","druidConfig": {"queryType": "timeseries","dataSource": "content-model-snapshot","intervals": "startdate/enddate","aggregations": [{"name": "visitors","type": "count","fieldName": "actor_id"}],"filters": [{"type": "equals","dimension": "context_cdata_id","value": "program_id"}, {"type": "equals","dimension": "context_cdata_type","value": "Program"}, {"type": "equals","dimension": "context_pdata_id","value": "'$producerEnv'.portal"}],"postAggregation": [],"descending": "false","limitSpec": {"type": "default","limit": 1000000,"columns": [{"dimension": "count","direction": "descending"}]}},"modelParams": {"contributionConfig":{"contentRequest":{"request": { "filters": {"programId": "programIdentifier","objectType": "content", "status": ["Draft","Live","Review"], "mimeType": "application/vnd.ekstep.content-collection"},"fields": ["acceptedContents", "rejectedContents"], "limit": 10000}},"correctionsPendingRequest":{"request": {"filters": {"objectType": "content","status": "Draft","prevStatus": "Live","programId": "programIdentifier","mimeType": {"!=": "application/vnd.ekstep.content-collection"},"contentType": {"!=": "Asset"}},"not_exists": ["sampleContent"],"facets":["createdBy"],"limit":0}},"contributionRequest":{"request": {"filters": {"objectType": "content", "status": ["Live"],"programId": "programIdentifier","mimeType": {"!=": "application/vnd.ekstep.content-collection"},"contentType": {"!=": "Asset"}},"not_exists": ["sampleContent"],"facets":["createdBy"],"limit":0}}},"reportConfig": {"id": "funnel_report","metrics": [],"labels": {"reportDate": "Report Generation Date","name": "Project Name"},"output": [],"outputs": [{"type": "csv","dims": ["identifier", "channel", "name"],"fileParameters": ["id", "dims"]}, {"type": "json","dims": ["identifier", "channel", "name"],"fileParameters": ["id", "dims"]}]},"store": "local","format": "csv","key": "druid-reports/","filePath": "druid-reports/","container": "test-container","folderPrefix": ["slug", "reportName"]},"output": [{"to": "console","params": {"printEvent": false}}],"parallelization": 8,"appName": "Funnel Metrics Report","deviceMapping": false}""".stripMargin
    val jobConfig = JSONUtils.deserialize[JobConfig](config)
    val configMap = JSONUtils.deserialize[Map[String,AnyRef]](JSONUtils.serialize(jobConfig))
    val data = FunnelReport.getContributionData(programId, configMap("modelParams").asInstanceOf[Map[String, AnyRef]])
    data._2 should be (0)
  }

}
