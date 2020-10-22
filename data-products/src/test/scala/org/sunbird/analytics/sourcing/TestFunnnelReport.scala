package org.sunbird.analytics.sourcing

import java.time.{ZoneOffset, ZonedDateTime}
import cats.syntax.either._
import ing.wbaa.druid.client.DruidClient
import ing.wbaa.druid._
import io.circe.Json
import io.circe.parser.parse
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.util.JSONUtils
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers
import org.sunbird.analytics.util.{EmbeddedPostgresql, SparkSpec}

import scala.concurrent.Future

class TestFunnnelReport extends SparkSpec with Matchers with MockFactory {
  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession()
    EmbeddedPostgresql.start()
    EmbeddedPostgresql.createProgramTable()
    EmbeddedPostgresql.createNominationTable()
  }

  override def afterAll() {
    super.afterAll()
    EmbeddedPostgresql.close()
  }

  it should "execute and generate Funnel Report" in {
    implicit val sc = spark.sparkContext
    implicit val mockFc = mock[FrameworkContext]

    //mocking for DruidDataFetcher
    import scala.concurrent.ExecutionContext.Implicits.global
    val json: String =
      """
        |{
        |    "identifier": "do_11298391390121984011",
        |    "visitors": 2.0
        |  }
      """.stripMargin

    val doc: Json = parse(json).getOrElse(Json.Null)
    val results = List(DruidResult.apply(Some(ZonedDateTime.of(2020, 1, 23, 17, 10, 3, 0, ZoneOffset.UTC)), doc))
    val druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.Timeseries)

    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.doQuery[DruidResponse](_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future(druidResponse)).anyNumberOfTimes()
    (mockFc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes()

    val config = """{"search": {"type": "none"},"model": "org.ekstep.analytics.job.report.FunnelReport","druidConfig": {"queryType": "timeseries","dataSource": "telemetry-events-syncts","intervals": "startdate/enddate","aggregations": [{"name": "visitors","type": "count","fieldName": "actor_id"}],"filters": [{"type": "equals","dimension": "context_cdata_id","value": "program_id"}, {"type": "equals","dimension": "context_cdata_type","value": "Program"}, {"type": "equals","dimension": "context_pdata_id","value": "'$producerEnv'.portal"}],"postAggregation": [],"descending": "false","limitSpec": {"type": "default","limit": 1000000,"columns": [{"dimension": "count","direction": "descending"}]}},"modelParams": {"reportConfig": {"id": "funnel_report","metrics": [],"labels": {"reportDate": "Report Generation Date","name": "Project Name"},"output": [],"outputs": [{"type": "csv","dims": ["identifier", "channel", "name"],"fileParameters": ["id", "dims"]}, {"type": "json","dims": ["identifier", "channel", "name"],"fileParameters": ["id", "dims"]}]},"store": "local","format": "csv","key": "druid-reports/","filePath": "druid-reports/","container": "test-container","folderPrefix": ["slug", "reportName"]},"output": [{"to": "console","params": {"printEvent": false}}],"parallelization": 8,"appName": "Funnel Metrics Report","deviceMapping": false}""".stripMargin
    val configMap = JSONUtils.deserialize[Map[String,AnyRef]](config)
    FunnelReport.generateFunnelReport(spark, configMap)
  }

}
