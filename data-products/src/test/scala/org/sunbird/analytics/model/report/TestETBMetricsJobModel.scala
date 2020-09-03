//package org.sunbird.analytics.model.report
//
//import java.time.{ZoneOffset, ZonedDateTime}
//
//import cats.syntax.either._
//import ing.wbaa.druid._
//import ing.wbaa.druid.client.DruidClient
//import io.circe.Json
//import io.circe.parser.parse
//import org.apache.spark.sql.SQLContext
//import org.ekstep.analytics.framework.FrameworkContext
//import org.sunbird.analytics.util.SparkSpec
//import org.scalamock.scalatest.MockFactory
//import org.scalatest.Matchers
//import org.sunbird.analytics.util.TextBookUtils
//import org.sunbird.analytics.model.report.ETBMetricsModel
//import org.ekstep.analytics.framework.util.{HTTPClient, JSONUtils, RestUtil}
//import org.sunbird.cloud.storage.BaseStorageService
//
//import scala.concurrent.Future
//import scala.io.Source
//
//class TestETBMetricsJobModel extends SparkSpec with Matchers with MockFactory {
//
//  override def beforeAll() = {
//    super.beforeAll()
//  }
//
//  "ETBMetricsModel" should "execute ETB Metrics model" in {
//    implicit val mockFc = mock[FrameworkContext]
//    val mockRestUtil = mock[HTTPClient]
//
//    val config = s"""{
//                    |	"reportConfig": {
//                    |		"id": "etb_metrics",
//                    |    "metrics" : [],
//                    |		"labels": {
//                    |			"date": "Date",
//                    |				"identifier": "TextBook ID",
//                    |       "name": "TextBook Name",
//                    |				"medium": "Medium",
//                    |				"gradeLevel": "Grade",
//                    |				"subject": "Subject",
//                    |       "createdOn": "Created On",
//                    |       "lastUpdatedOn": "Last Updated On",
//                    |       "totalQRCodes": "Total number of QR codes",
//                    |       "contentLinkedQR": "Number of QR codes with atleast 1 linked content",
//                    |       "withoutContentQR": "Number of QR codes with no linked content",
//                    |       "withoutContentT1": "Term 1 QR Codes with no linked content",
//                    |       "withoutContentT2": "Term 2 QR Codes with no linked content",
//                    |       "status": "Status",
//                    |       "totalContentLinked": "Total content linked",
//                    |       "totalQRLinked": "Total QR codes linked to content",
//                    |       "totalQRNotLinked": "Total number of QR codes with no linked content",
//                    |       "leafNodesCount": "Total number of leaf nodes",
//                    |       "leafNodeUnlinked": "Number of leaf nodes with no content"
//                    |		},
//                    |  "dateRange": {
//                    |				"staticInterval": "Last7Days",
//                    |				"granularity": "all"
//                    |			},
//                    |		"output": [{
//                    |			"type": "csv",
//                    |			"dims": ["identifier", "channel", "name"],
//                    |			"fileParameters": ["id", "dims"]
//                    |		}]
//                    |	},
//                    | "dialcodeReportConfig":{
//                    | "id": "etb_metrics",
//                    |    "metrics" : [],
//                    |		"labels": {},
//                    |  "output": [{
//                    |			"type": "csv",
//                    |			"dims": ["identifier", "channel", "name"],
//                    |			"fileParameters": ["id", "dims"]
//                    |		}]
//                    | },
//                    | "etbFileConfig": {
//                    | "bucket": "test-container",
//                    | "file": "druid-reports/etb_metrics/dialcode_counts.csv",
//                    | "filePath": "src/test/resources/reports/dialcode_counts.csv"
//                    | },
//                    | "druidConfig": {"queryType": "groupBy","dataSource": "content-model-snapshot","intervals": "1901-01-01T00:00:00+00:00/2101-01-01T00:00:00+00:00","aggregations": [{"name": "count","type": "count"}],"dimensions": [{"fieldName": "channel","aliasName": "channel"},{"fieldName": "identifier","aliasName": "identifier"},{"fieldName": "name","aliasName": "name"},{"fieldName": "createdFor","aliasName": "createdFor"},{"fieldName": "createdOn","aliasName": "createdOn"},{"fieldName": "lastUpdatedOn","aliasName": "lastUpdatedOn"},{"fieldName": "board","aliasName": "board"},{"fieldName": "medium","aliasName": "medium"},{"fieldName": "gradeLevel","aliasName": "gradeLevel"},{"fieldName": "subject","aliasName": "subject"},{"fieldName": "status","aliasName": "status"}],"filters": [{"type": "equals","dimension": "contentType","value": "TextBook"}],"postAggregation": [],"descending": "false","limitSpec": {"type": "default","limit": 1000000,"columns": [{"dimension": "count","direction": "descending"}]}},
//                    |"tenantConfig": {
//                    |"tenantId":"",
//                    |  "slugName":""
//                    |	},
//                    |	"key": "druid-reports/",
//                    |  "format":"csv",
//                    |	"filePath": "druid-reports/",
//                    |	"container": "test-container",
//                    |	"folderPrefix": ["slug", "reportName"],
//                    | "store": "local"
//                    |}""".stripMargin
//    val jobConfig = JSONUtils.deserialize[Map[String, AnyRef]](config)
//
//    implicit val sqlContext = new SQLContext(sc)
//    import sqlContext.implicits._
//
//    //mocking for DruidDataFetcher
//    import scala.concurrent.ExecutionContext.Implicits.global
//    val json: String =
//      """
//        |{
//        |    "identifier": "do_11298391390121984011",
//        |    "channel": "0124698765480987654",
//        |    "status": "Live",
//        |    "name": "Kayal_Book",
//        |    "date": "2020-03-25",
//        |    "dialcode": "BSD1AV",
//        |    "scans": 2.0
//        |  }
//      """.stripMargin
//
//    val doc: Json = parse(json).getOrElse(Json.Null)
//    val results = List(DruidResult.apply(Some(ZonedDateTime.of(2020, 1, 23, 17, 10, 3, 0, ZoneOffset.UTC)), doc))
//    val druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.GroupBy)
//
//    implicit val mockDruidConfig = DruidConfig.DefaultConfig
//    val mockDruidClient = mock[DruidClient]
//    (mockDruidClient.doQuery[DruidResponse](_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future(druidResponse)).anyNumberOfTimes()
//    (mockFc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes()
//
//    val resultRDD = ETBMetricsModel.execute(sc.emptyRDD, Option(jobConfig))
//
//  }
//
//  "ETBMetricsModel" should "save files with deltaPath" in {
//    implicit val mockFc = mock[FrameworkContext]
//    val mockRestUtil = mock[HTTPClient]
//
//    val config = s"""{"reportConfig": {"id": "etb_metrics","metrics": [],"labels": {"date": "Date","identifier": "TextBook ID","name": "TextBook Name","medium": "Medium","gradeLevel": "Grade","subject": "Subject","createdOn": "Created On","lastUpdatedOn": "Last Updated On","totalQRCodes": "Total number of QR codes","contentLinkedQR": "Number of QR codes with atleast 1 linked content","withoutContentQR": "Number of QR codes with no linked content","withoutContentT1": "Term 1 QR Codes with no linked content","withoutContentT2": "Term 2 QR Codes with no linked content","status": "Status","totalContentLinked": "Total content linked","totalQRLinked": "Total QR codes linked to content","totalQRNotLinked": "Total number of QR codes with no linked content","leafNodesCount": "Total number of leaf nodes","leafNodeUnlinked": "Number of leaf nodes with no content"},"dateRange": {"staticInterval": "Last7Days","granularity": "all"},"output": [{"type": "csv","dims": ["identifier", "channel", "name"],"fileParameters": ["id", "dims"]}],"mergeConfig": {"postContainer": "test","frequency": "WEEK","basePath": "baseScriptPath","rollup": 1,"reportPath": "dialcode_counts.csv","rollupAge": "year","rollupCol": "Date","rollupRange": 1}},"dialcodeReportConfig": {"id": "etb_metrics","metrics": [],"labels": {},"output": [{"type": "csv","dims": ["identifier", "channel", "name"],"fileParameters": ["id", "dims"]}]},"etbFileConfig": {"bucket": "test-container","file": "druid-reports/etb_metrics/dialcode_counts.csv","filePath": "src/test/resources/reports/dialcode_counts.csv"},"druidConfig": {"queryType": "groupBy","dataSource": "content-model-snapshot","intervals": "1901-01-01T00:00:00+00:00/2101-01-01T00:00:00+00:00","aggregations": [{"name": "count","type": "count"}],"dimensions": [{"fieldName": "channel","aliasName": "channel"}, {"fieldName": "identifier","aliasName": "identifier"}, {"fieldName": "name","aliasName": "name"}, {"fieldName": "createdFor","aliasName": "createdFor"}, {"fieldName": "createdOn","aliasName": "createdOn"}, {"fieldName": "lastUpdatedOn","aliasName": "lastUpdatedOn"}, {"fieldName": "board","aliasName": "board"}, {"fieldName": "medium","aliasName": "medium"}, {"fieldName": "gradeLevel","aliasName": "gradeLevel"}, {"fieldName": "subject","aliasName": "subject"}, {"fieldName": "status","aliasName": "status"}],"filters": [{"type": "equals","dimension": "contentType","value": "TextBook"}],"postAggregation": [],"descending": "false","limitSpec": {"type": "default","limit": 1000000,"columns": [{"dimension": "count","direction": "descending"}]}},"tenantConfig": {"tenantId": "","slugName": ""},"key": "druid-reports/","format": "csv","filePath": "druid-reports/","container": "test-container","folderPrefix": ["slug", "reportName"],"store": "local"}""".stripMargin
//    val jobConfig = JSONUtils.deserialize[Map[String, AnyRef]](config)
//
//    implicit val sqlContext = new SQLContext(sc)
//    import sqlContext.implicits._
//
//    //mocking for DruidDataFetcher
//    import scala.concurrent.ExecutionContext.Implicits.global
//    val json: String =
//      """
//        |{
//        |    "identifier": "do_11298391390121984011",
//        |    "channel": "0124698765480987654",
//        |    "status": "Live",
//        |    "name": "Kayal_Book",
//        |    "date": "2020-03-25",
//        |    "dialcode": "BSD1AV",
//        |    "scans": 2.0
//        |  }
//      """.stripMargin
//
//    val doc: Json = parse(json).getOrElse(Json.Null)
//    val results = List(DruidResult.apply(Some(ZonedDateTime.of(2020, 1, 23, 17, 10, 3, 0, ZoneOffset.UTC)), doc))
//    val druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.GroupBy)
//
//    implicit val mockDruidConfig = DruidConfig.DefaultConfig
//    val mockDruidClient = mock[DruidClient]
//    (mockDruidClient.doQuery[DruidResponse](_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future(druidResponse)).anyNumberOfTimes()
//    (mockFc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes()
//
//    the[Exception] thrownBy {
//      ETBMetricsModel.execute(sc.emptyRDD, Option(jobConfig))
//    } should have message "Merge report script failed with exit code 127"
//
//  }
//
//  it should "generate chapter reports for dce dialcode" in {
//    implicit val mockFc = mock[FrameworkContext]
//    val mockRestUtil = mock[HTTPClient]
//
//    val hierarchyData = JSONUtils.deserialize[ContentDetails](Source.fromInputStream
//    (getClass.getResourceAsStream("/reports/hierarchyData.json")).getLines().mkString).result.content
//
//    val textBookData = JSONUtils.deserialize[TextbookData](Source.fromInputStream
//    (getClass.getResourceAsStream("/reports/textbookDetails.json")).getLines().mkString)
//
//    implicit val sqlContext = new SQLContext(sc)
//    import sqlContext.implicits._
//
//    //mocking for DruidDataFetcher
//    import scala.concurrent.ExecutionContext.Implicits.global
//    val json: String =
//      """
//        |{
//        |    "identifier": "do_113031279120924672120",
//        |    "channel": "0124698765480987654",
//        |    "status": "Live",
//        |    "name": "Kayal_Book",
//        |    "date": "2020-03-25",
//        |    "dialcode": "BSD1AV",
//        |    "scans": 2.0
//        |  }
//      """.stripMargin
//
//    val doc: Json = parse(json).getOrElse(Json.Null)
//    val results = List(DruidResult.apply(Some(ZonedDateTime.of(2020, 1, 23, 17, 10, 3, 0, ZoneOffset.UTC)), doc))
//    val druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.GroupBy)
//
//    implicit val mockDruidConfig = DruidConfig.DefaultConfig
//    val mockDruidClient = mock[DruidClient]
//    (mockDruidClient.doQuery[DruidResponse](_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future(druidResponse)).anyNumberOfTimes()
//    (mockFc.getDruidRollUpClient _).expects().returns(mockDruidClient).anyNumberOfTimes()
//
//    val report = TextBookUtils.generateDCEDialCodeReport(hierarchyData,textBookData)
//    report._1.length should be(6)
//  }
//
//}
