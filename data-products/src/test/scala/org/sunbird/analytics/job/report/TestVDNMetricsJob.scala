package org.sunbird.analytics.job.report

import java.time.{ZoneOffset, ZonedDateTime}
import cats.syntax.either._
import ing.wbaa.druid.client.DruidClient
import ing.wbaa.druid.{DruidConfig, DruidQuery, DruidResponse, DruidResponseTimeseriesImpl, DruidResult, QueryType}
import io.circe.Json
import io.circe.parser.parse
import org.ekstep.analytics.framework.FrameworkContext
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers
import org.sunbird.analytics.util.SparkSpec

import scala.concurrent.Future

class TestVDNMetricsJob extends SparkSpec with Matchers with MockFactory {
  val spark = getSparkSession()

  it should "execute generate VDN metrics report" in {
    implicit val mockFc = mock[FrameworkContext]

    //mocking for DruidDataFetcher
    import scala.concurrent.ExecutionContext.Implicits.global
    val json: String =
      """
        |{
        |    "identifier": "do_11298391390121984011",
        |    "channel": "0124698765480987654",
        |    "status": "Live",
        |    "name": "Kayal_Book",
        |    "date": "2020-03-25",
        |    "dialcode": "BSD1AV",
        |    "scans": 2.0
        |  }
      """.stripMargin

    val doc: Json = parse(json).getOrElse(Json.Null)
    val results = List(DruidResult.apply(Some(ZonedDateTime.of(2020, 1, 23, 17, 10, 3, 0, ZoneOffset.UTC)), doc))
    val druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.GroupBy)

    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.doQuery[DruidResponse](_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future(druidResponse)).anyNumberOfTimes()
    (mockFc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes()

    val config = """{"search": {"type": "none"},"druidConfig": {"queryType": "groupBy","dataSource": "content-model-snapshot","intervals": "1901-01-01T00:00:00+00:00/2101-01-01T00:00:00+00:00","aggregations": [{"name": "count","type": "count"}],"dimensions": [{"fieldName": "channel","aliasName": "channel"}, {"fieldName": "identifier","aliasName": "identifier","type": "Extraction","outputType": "STRING","extractionFn": [{"type": "javascript","fn": "function(str){return str == null ? null: str.split(\".\")[0]}"}]}, {"fieldName": "name","aliasName": "name"}, {"fieldName": "createdFor","aliasName": "createdFor"}, {"fieldName": "createdOn","aliasName": "createdOn"}, {"fieldName": "lastUpdatedOn","aliasName": "lastUpdatedOn"}, {"fieldName": "board","aliasName": "board"}, {"fieldName": "medium","aliasName": "medium"}, {"fieldName": "gradeLevel","aliasName": "gradeLevel"}, {"fieldName": "subject","aliasName": "subject"}, {"fieldName": "status","aliasName": "status"}],"filters": [{"type": "equals","dimension": "contentType","value": "TextBook"}, {"type": "equals","dimension": "channel","value": "01306475770753843226"}, {"type": "in","dimension": "status","values": ["Live", "Draft", "Review"]}],"postAggregation": [],"descending": "false","limitSpec": {"type": "default","limit": 1000000,"columns": [{"dimension": "count","direction": "descending"}]}},"model": "org.ekstep.analytics.job.report.VDNMetricsJob","modelParams": {"reportConfig": {"id": "vdn_report","metrics": [],"labels": {"date": "Date","name": "Textbook Name","medium": "Medium","gradeLevel": "Grade","subject": "Subject","board": "Board","grade": "Grade","chapters": "Chapter Name","totalChapters": "Total number of chapters (first level sections of ToC)","status": "Textbook Status"},"output":[],"outputs": [{"type": "csv","dims": ["identifier", "channel", "name"],"fileParameters": ["id", "dims"]},{"type": "json","dims": ["identifier", "channel", "name"],"fileParameters": ["id", "dims"]}]},"store": "local","format": "csv","key": "druid-reports/","filePath": "druid-reports/","container": "test-container","folderPrefix": ["slug", "reportName"]},"output": [{"to": "console","params": {"printEvent": false}}],"parallelization": 8,"appName": "VDN Metrics Job","deviceMapping": false}""".stripMargin
    VDNMetricsJob.generateVDNReport(spark, config)

  }

}
