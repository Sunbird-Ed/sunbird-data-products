package org.sunbird.analytics.sourcing

import java.time.{ZoneOffset, ZonedDateTime}

import cats.syntax.either._
import ing.wbaa.druid.client.DruidClient
import ing.wbaa.druid._
import io.circe.Json
import io.circe.parser.parse
import org.apache.spark.sql.SparkSession
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

    val config = """{"search": {"type": "none"},"model": "org.ekstep.analytics.job.report.FunnelReport","druidConfig": {"queryType": "timeseries","dataSource": "telemetry-events-syncts","intervals": "startdate/enddate","aggregations": [{"name": "visitors","type": "count","fieldName": "actor_id"}],"filters": [{"type": "equals","dimension": "context_cdata_id","value": "program_id"}, {"type": "equals","dimension": "context_cdata_type","value": "Program"}, {"type": "equals","dimension": "context_pdata_id","value": "'$producerEnv'.portal"}],"postAggregation": [],"descending": "false","limitSpec": {"type": "default","limit": 1000000,"columns": [{"dimension": "count","direction": "descending"}]}},"modelParams": {"reportConfig": {"id": "funnel_report","metrics": [],"labels": {"reportDate": "Report Generation Date","name": "Project Name"},"output": [],"outputs": [{"type": "csv","dims": ["identifier", "channel", "name"],"fileParameters": ["id", "dims"]}, {"type": "json","dims": ["identifier", "channel", "name"],"fileParameters": ["id", "dims"]}]},"store": "local","format": "csv","key": "druid-reports/","filePath": "druid-reports/","container": "test-container","folderPrefix": ["slug", "reportName"]},"output": [{"to": "console","params": {"printEvent": false}}],"parallelization": 8,"appName": "Funnel Metrics Report","deviceMapping": false}""".stripMargin
    val configMap = JSONUtils.deserialize[Map[String,AnyRef]](config)
    implicit val jobConfig = JSONUtils.deserialize[JobConfig](config)
    implicit val sparkCon = spark
    val reportMetrics = FunnelReport.execute()
    reportMetrics.getOrElse("funnelReportCount",0) should be (2)
  }

  it should "return 0 if no resources found for correction pending" in {
    val programId = "do_123"
    val data = FunnelReport.getContributionData(programId)
    data._2 should be (0)
  }

}
