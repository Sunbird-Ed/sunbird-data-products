package org.sunbird.analytics.job.report

import java.time.{ZoneOffset, ZonedDateTime}

import cats.syntax.either._
import ing.wbaa.druid.client.DruidClient
import ing.wbaa.druid.{DruidConfig, DruidQuery, DruidResponse, DruidResponseTimeseriesImpl, DruidResult, QueryType}
import io.circe.Json
import io.circe.parser.parse
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{Dispatcher, DruidQueryModel, Fetcher, FrameworkContext, JobConfig, Query}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.util.{EmbeddedCassandra, SparkSpec}

import scala.concurrent.Future

class TestAssessmentCorrectionJob extends SparkSpec(null) with MockFactory{

  override def beforeAll(): Unit = {
    super.beforeAll()
    // embedded cassandra setup
    EmbeddedCassandra.loadData("src/test/resources/assessment-correction-report/assessment_report_data.cql") // Load test data in embedded cassandra server

  }
  override def afterAll() : Unit = {
    super.afterAll();
    EmbeddedCassandra.close()
  }
  "AssessmentCorrectionJob" should "execute AssessmentCorrectionJob job and won't throw any error" in {
    implicit val mockFc = mock[FrameworkContext]
    val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/assessment-correction/test-data.log"))))), null, null , "org.sunbird.analytics.model.report.AssessmentCorrectionModel", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestAssessmentCorrection"), Option(true))
    val strConfig = """{"model":"org.sunbird.analytics.model.report.AssessmentCorrectionModel","search":{"type":"local","queries":[{"file":"src/test/resources/assessment-correction/test-data.log"}]},"modelParams":{"parallelization":200,"druidConfig":{"queryType":"groupBy","dataSource":"content-model-snapshot","intervals":"LastDay","granularity":"all","aggregations":[{"name":"count","type":"count","fieldName":"count"}],"dimensions":[{"fieldName":"identifier","aliasName":"identifier"}],"filters":[{"type":"equals","dimension":"contentType","value":"SelfAssess"}],"descending":"false"}},"output":[{"to":"file","params":{"file":"src/test/resources/assessment-correction"}}],"parallelization":8,"appName":"Assessment Correction Model"}"""
    val data = loadFile[String]("src/test/resources/assessment-correction/test-data1.log");

    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    val druidConfig = JSONUtils.deserialize[DruidQueryModel](JSONUtils.serialize(jobConfig.modelParams.get("druidConfig")))
    //mocking for DruidDataFetcher
    import scala.concurrent.ExecutionContext.Implicits.global
    val json: String =
      """
        |{
        |  "identifier": "do_2130252196790026241869"
        |}
      """.stripMargin

    val doc: Json = parse(json).getOrElse(Json.Null)
    val results = List(DruidResult.apply(Some(ZonedDateTime.of(2020, 1, 23, 17, 10, 3, 0, ZoneOffset.UTC)), doc));
    val druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.GroupBy)

    implicit val mockDruidConfig: DruidConfig = DruidConfig.DefaultConfig

    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.doQuery[DruidResponse](_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future(druidResponse)).anyNumberOfTimes()
    (mockFc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes()


    AssessmentCorrectionJob.main(JSONUtils.serialize(config))(Option(sc))
  }
}
