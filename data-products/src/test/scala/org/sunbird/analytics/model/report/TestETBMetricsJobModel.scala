package org.sunbird.analytics.model.report

import org.apache.spark.sql.SQLContext
import org.ekstep.analytics.framework.FrameworkContext
import org.sunbird.analytics.util.SparkSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers
import org.sunbird.analytics.util.TextBookUtils
import org.sunbird.analytics.model.report.ETBMetricsModel
import org.ekstep.analytics.framework.util.{HTTPClient, JSONUtils, RestUtil}
import org.sunbird.cloud.storage.BaseStorageService

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
                    |    "metrics" : [],
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
                    |		"output": [{
                    |			"type": "csv",
                    |			"dims": ["identifier", "channel", "name"],
                    |			"fileParameters": ["id", "dims"]
                    |		}]
                    |	},
                    | "esConfig": {
                    |"request": {
                    |   "filters": {
                    |       "contentType": ["Textbook"],
                    |       "identifier":[],
                    |       "status": ["Live", "Review", "Draft"]
                    |   },
                    |   "sort_by": {"createdOn":"desc"},
                    |   "limit": 50
                    | }
                    |},
                    |"tenantConfig": {
                    |"tenantId":"",
                    |  "slugName":"mitra"
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
    val request = s"""{"request":{"filters":{"contentType":["Textbook"],"identifier":[],"status":["Live","Review","Draft"]},"sort_by":{"createdOn":"desc"},"limit":50}}""".stripMargin

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
                          |        "filters": {"slug":"mitra"},
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

    val resultRDD = ETBMetricsModel.execute(sc.emptyRDD, Option(jobConfig))

  }

}
