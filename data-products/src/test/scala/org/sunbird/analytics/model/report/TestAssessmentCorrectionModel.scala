package org.sunbird.analytics.model.report

import java.time.{ZoneOffset, ZonedDateTime}

import cats.syntax.either._
import ing.wbaa.druid._
import ing.wbaa.druid.client.DruidClient
import io.circe.Json
import io.circe.parser.parse
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{DruidQueryModel, FrameworkContext, JobConfig}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.util.{EmbeddedCassandra, SparkSpec}

import scala.concurrent.Future

class TestAssessmentCorrectionModel extends SparkSpec(null) with MockFactory {

  implicit val mockFc = mock[FrameworkContext]

  override def beforeAll(): Unit = {
    super.beforeAll()
    // embedded cassandra setup
    EmbeddedCassandra.loadData("src/test/resources/assessment-correction-report/assessment_report_data.cql") // Load test data in embedded cassandra server

  }
  override def afterAll() : Unit = {
    super.afterAll();
    EmbeddedCassandra.close()
  }

  "AssessmentCorrectionModel" should "provide the event" in {
    val strConfig = """{"model":"org.sunbird.analytics.model.report.AssessmentCorrectionModel","search":{"type":"local","queries":[{"file":"src/test/resources/assessment-correction-report/test-data.log"}]},"modelParams":{"batchId":[],"parallelization":200,"druidConfig":{"queryType":"groupBy","dataSource":"content-model-snapshot","intervals":"LastDay","granularity":"all","aggregations":[{"name":"count","type":"count","fieldName":"count"}],"dimensions":[{"fieldName":"identifier","aliasName":"identifier"}],"filters":[{"type":"equals","dimension":"contentType","value":"SelfAssess"}],"descending":"false"}},"output":[{"to":"file","params":{"file":"src/test/resources/assessment-correction"}}],"parallelization":8,"appName":"Assessment Correction Model"}"""
    val strConfig1 = """{"batchId":[],"parallelization":200,"druidConfig":{"queryType":"groupBy","dataSource":"content-model-snapshot","intervals":"LastDay","granularity":"all","aggregations":[{"name":"count","type":"count","fieldName":"count"}],"dimensions":[{"fieldName":"identifier","aliasName":"identifier"}],"filters":[{"type":"equals","dimension":"contentType","value":"SelfAssess"}],"descending":"false"},"fileOutputConfig":{"to":"file","params":{"file":"src/test/resources/assessment-correction/failedEvents"}}}"""
    val data = loadFile[String]("src/test/resources/assessment-correction-report/test-data2.log");

    val config = JSONUtils.deserialize[JobConfig](strConfig)
    val druidConfig = JSONUtils.deserialize[DruidQueryModel](JSONUtils.serialize(config.modelParams.get("druidConfig")))
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
    (mockFc.getDruidRollUpClient _).expects().returns(mockDruidClient).anyNumberOfTimes()

    val mapConfig = JSONUtils.deserialize[Map[String, AnyRef]](strConfig1)
    val output = AssessmentCorrectionModel.execute(data, Option(mapConfig))

    output.count() should be (1)
    val assessEvent = output.collect()
    assessEvent.map {f => f.batchId }.toList should contain allElementsOf List("batch-001")
    assessEvent.map {f => f.courseId }.toList should contain allElementsOf List("do_2130694768149053441103")
    assessEvent.map {f => f.userId }.toList should contain allElementsOf List("08631a74-4b94-4cf7-a818-831135248a4a")
    assessEvent.map {f => f.contentId }.toList should contain allElementsOf List("do_2130252196790026241869")

    val events = assessEvent.map {f=> f.events}.head
    events.size should be (9)
    JSONUtils.serialize(events.head) should be ("""{"eid":"ASSESS","ets":1595396274835,"ver":"3.1","mid":"ASSESS:3a2fefaf3b8db7ddbb2c41c3d28ddebb","actor":{"id":"08631a74-4b94-4cf7-a818-831135248a4a","type":"User"},"context":{"channel":"01269878797503692810","pdata":{"id":"preprod.diksha.portal","ver":"3.0.4","pid":"sunbird-portal.contentplayer","model":null},"env":"contentplayer","sid":"x9uYc2nb7NJOWlqPH5S0bY4YfgFZtF04","did":"ceec93de99b72cfd08e3f679418c2c3b","cdata":[{"id":"1b3fe62e5c6212a5332700d1e9ded4b4","type":"AttemptId"},{"id":"do_2130694768154214401105","type":"course"},{"id":"01306947998293196823","type":"batch"},{"id":"eb4bb0ae270af59b7e4f350b0e937486","type":"ContentSession"},{"id":"6044429e53cfbd6d528d85cc24d06894","type":"PlaySession"}],"rollup":{"l1":"01269878797503692810"}},"object":{"id":"do_2130252196790026241869","type":"Content","ver":"1","rollup":{"l1":"do_2130694768149053441103","l2":"do_2130694768154214401105","l3":"do_2130252196790026241869"},"subtype":null,"parent":null},"edata":{"duration":1,"item":{"id":"do_2130251387497431041150","maxscore":1,"exlength":0,"params":[{"1":"{\"text\":\"pink\\n\"}"},{"2":"{\"text\":\"red\\n\"}"},{"answer":"{\"correct\":[\"1\"]}"}],"uri":"","desc":"","title":"which color is good ?\n","mmc":[],"mc":[]},"pass":"No","score":0,"resvalues":[],"rating":0.0,"index":3,"data":null,"sort":null,"correlationid":null,"filters":null,"size":0},"tags":["01269878797503692810"],"flags":{"derived_location_retrieved":false,"device_data_retrieved":false,"user_data_retrieved":false,"dialcode_data_retrieved":false,"content_data_retrieved":false,"collection_data_retrieved":false},"@timestamp":"2020-07-22T05:38:00.120Z"}""")
  }

  it should "generate events for a specific batch" in {
    val strConfig = """{"model":"org.sunbird.analytics.model.report.AssessmentCorrectionModel","search":{"type":"local","queries":[{"file":"src/test/resources/assessment-correction-report/test-data.log"}]},"modelParams":{"batchId":"batch-001","parallelization":200,"druidConfig":{"queryType":"groupBy","dataSource":"content-model-snapshot","intervals":"LastDay","granularity":"all","aggregations":[{"name":"count","type":"count","fieldName":"count"}],"dimensions":[{"fieldName":"identifier","aliasName":"identifier"}],"filters":[{"type":"equals","dimension":"contentType","value":"SelfAssess"}],"descending":"false"}},"output":[{"to":"file","params":{"file":"src/test/resources/assessment-correction"}}],"parallelization":8,"appName":"Assessment Correction Model"}"""
    val strConfig1 = """{"batchId":["batch-002"], "parallelization":200,"druidConfig":{"queryType":"groupBy","dataSource":"content-model-snapshot","intervals":"LastDay","granularity":"all","aggregations":[{"name":"count","type":"count","fieldName":"count"}],"dimensions":[{"fieldName":"identifier","aliasName":"identifier"}],"filters":[{"type":"equals","dimension":"contentType","value":"SelfAssess"}],"descending":"false"}}"""
    val data = loadFile[String]("src/test/resources/assessment-correction-report/test-data1.log");

    val config = JSONUtils.deserialize[JobConfig](strConfig)
    val druidConfig = JSONUtils.deserialize[DruidQueryModel](JSONUtils.serialize(config.modelParams.get("druidConfig")))
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
    (mockFc.getDruidRollUpClient _).expects().returns(mockDruidClient).anyNumberOfTimes()

    val mapConfig = JSONUtils.deserialize[Map[String, AnyRef]](strConfig1)
    val output = AssessmentCorrectionModel.execute(data, Option(mapConfig))
  }
}
