package org.sunbird.analytics.model.report

import java.time.{ZoneOffset, ZonedDateTime}
import cats.syntax.either._
import ing.wbaa.druid.{DruidConfig, DruidQuery, DruidResponse, DruidResult, QueryType}
import ing.wbaa.druid.client.DruidClient
import io.circe.Json
import io.circe.parser.parse
import org.apache.spark.sql.SQLContext
import org.ekstep.analytics.framework.FrameworkContext
import org.sunbird.analytics.util.SparkSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers
import org.sunbird.analytics.util.TextBookUtils
import org.sunbird.analytics.model.report.ETBMetricsModel
import org.ekstep.analytics.framework.util.{HTTPClient, JSONUtils, RestUtil}
import org.sunbird.cloud.storage.BaseStorageService

import scala.concurrent.Future
import scala.io.Source

class TestETBMetricsJobModel extends SparkSpec with Matchers with MockFactory {

  override def beforeAll() = {
    super.beforeAll()
  }

  "ETBMetricsModel" should "execute ETB Metrics model" in {
    implicit val mockFc = mock[FrameworkContext]
    val mockRestUtil = mock[HTTPClient]

    val mockStorageService = mock[BaseStorageService]
    (mockFc.getStorageService(_: String)).expects("azure").returns(mockStorageService).anyNumberOfTimes();
    (mockStorageService.upload (_: String, _: String, _: String, _: Option[Boolean], _: Option[Int], _: Option[Int], _: Option[Int])).expects(*, *, *, *, *, *, *).returns("").anyNumberOfTimes();
    (mockStorageService.closeContext _).expects().returns().anyNumberOfTimes()

    val config = s"""{
                    |	"reportConfig": {
                    |		"id": "etb_metrics",
                    |    "metrics" : [{
                    |    "metric": "totalQRScans",
                    |		 "label": "Total QR Scans",
                    |    "druidQuery": {
                    |      "queryType": "groupBy",
                    |			 "dataSource": "telemetry-events",
                    |			 "intervals": "Last7Days",
                    |    "aggregations": [
                    |            {
                    |                "type": "count",
                    |                "name": "scans"
                    |            }
                    |        ],
                    |        "granularity": "all",
                    |        "postAggregations": [],
                    |        "filter": {
                    |    "type": "and",
                    |    "fields": [
                    |      {
                    |        "type": "not",
                    |        "field": {
                    |          "type": "selector",
                    |          "dimension": "object_id",
                    |          "value": null
                    |        }
                    |      },
                    |      {
                    |        "type": "selector",
                    |        "dimension": "eid",
                    |        "value": "SEARCH"
                    |      }
                    |    ]
                    |  },
                    |  "dimensions": [
                    |            {
                    |						"fieldName": "object_id",
                    |						"aliasName": "dialcode"
                    |					}
                    |        ]
                    |   }
                    |    }],
                    |		"labels": {
                    |			"date": "Date",
                    |				"identifier": "TextBook ID",
                    |       "name": "TextBook Name",
                    |				"medium": "Medium",
                    |				"gradeLevel": "Grade",
                    |				"subject": "Subject",
                    |       "createdOn": "Created On",
                    |       "lastUpdatedOn": "Last Updated On",
                    |       "totalQRCodes": "Total number of QR codes",
                    |       "contentLinkedQR": "Number of QR codes with atleast 1 linked content",
                    |       "withoutContentQR": "Number of QR codes with no linked content",
                    |       "withoutContentT1": "Term 1 QR Codes with no linked content",
                    |       "withoutContentT2": "Term 2 QR Codes with no linked content",
                    |       "status": "Status",
                    |       "totalContentLinked": "Total content linked",
                    |       "totalQRLinked": "Total QR codes linked to content",
                    |       "totalQRNotLinked": "Total number of QR codes with no linked content",
                    |       "leafNodesCount": "Total number of leaf nodes",
                    |       "leafNodeUnlinked": "Number of leaf nodes with no content"
                    |		},
                    |  "dateRange": {
                    |				"staticInterval": "Last7Days",
                    |				"granularity": "all"
                    |			},
                    |		"output": [{
                    |			"type": "csv",
                    |			"dims": ["identifier", "channel", "name"],
                    |			"fileParameters": ["id", "dims"]
                    |		}]
                    |	},
                    | "etbFileConfig": {
                    | "bucket": "test-container",
                    | "file": "druid-reports/etb_metrics/dialcode_counts.csv",
                    | "filePath": "src/test/resources/reports/dialcode_counts.csv"
                    | },
                    | "esConfig": {
                    |"request": {
                    |   "filters": {
                    |       "contentType": ["Textbook"],
                    |       "identifier":["do_11298391390121984011","do_11298386993685299212","do_112976283013464064115","do_112984096876756992153","do_11298420612304896011"],
                    |       "status": ["Live", "Review", "Draft"]
                    |   },
                    |   "sort_by": {"createdOn":"desc"},
                    |   "limit": 10
                    | }
                    |},
                    |"tenantConfig": {
                    |"tenantId":"",
                    |  "slugName":""
                    |	},
                    |	"key": "druid-reports/",
                    |  "format":"csv",
                    |	"filePath": "druid-reports/",
                    |	"container": "test-container",
                    |	"folderPrefix": ["slug", "reportName"],
                    | "store": "local"
                    |}""".stripMargin
    val jobConfig = JSONUtils.deserialize[Map[String, AnyRef]](config)

    //Mock for composite search
    val textBookData = JSONUtils.deserialize[TextBookDetails](Source.fromInputStream
    (getClass.getResourceAsStream("/reports/textbookDetails.json")).getLines().mkString)
    val request = s"""{"request":{"filters":{"contentType":["Textbook"],"identifier":["do_11298391390121984011","do_11298386993685299212","do_112976283013464064115","do_112984096876756992153","do_11298420612304896011"],"status":["Live","Review","Draft"]},"sort_by":{"createdOn":"desc"},"limit":10}}""".stripMargin

    (mockRestUtil.post[TextBookDetails](_: String, _: String, _: Option[Map[String,String]])(_: Manifest[TextBookDetails]))
      .expects("https://dev.sunbirded.org/action/composite/v3/search", v2 = request, None,*)
      .returns(textBookData)

    val res = TextBookUtils.getTextBooks(jobConfig,mockRestUtil)

    //Mock for Tenant Info
    val tenantInfo = JSONUtils.deserialize[TenantResponse](Source.fromInputStream
    (getClass.getResourceAsStream("/reports/tenantInfo.json")).getLines().mkString)

    val tenantRequest = """{
                          |    "params": { },
                          |    "request":{
                          |        "filters": {"isRootOrg":"true"},
                          |        "offset": 0,
                          |        "limit": 1000,
                          |        "fields": ["id", "channel", "slug", "orgName"]
                          |    }
                          |}""".stripMargin
    (mockRestUtil.post[TenantResponse](_: String, _: String, _: Option[Map[String,String]])(_: Manifest[TenantResponse]))
      .expects("https://dev.sunbirded.org/api/org/v1/search", v2 = tenantRequest, None,*)
      .returns(tenantInfo)

    val resp = ETBMetricsModel.getTenantInfo(jobConfig,mockRestUtil)
    implicit val httpClient = RestUtil
    val finalRes = TextBookUtils.getTextbookHierarchy(res,resp,httpClient)
    val etb = finalRes.map(k=>k.etb.get)
    val dce = finalRes.map(k=>k.dce.get)

    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //mocking for DruidDataFetcher
    import scala.concurrent.ExecutionContext.Implicits.global
    val json: String =
      """
        |{
        |    "date": "2020-03-25",
        |    "dialcode": "BSD1AV",
        |    "scans": 2.0
        |  }
      """.stripMargin

    val doc: Json = parse(json).getOrElse(Json.Null)
    val results = List(DruidResult.apply(ZonedDateTime.of(2020, 1, 23, 17, 10, 3, 0, ZoneOffset.UTC), doc))
    val druidResponse = DruidResponse.apply(results, QueryType.GroupBy)

    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.doQuery(_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future(druidResponse)).anyNumberOfTimes()
    (mockFc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes()

    val resultRDD = ETBMetricsModel.execute(sc.emptyRDD, Option(jobConfig))

  }

}
