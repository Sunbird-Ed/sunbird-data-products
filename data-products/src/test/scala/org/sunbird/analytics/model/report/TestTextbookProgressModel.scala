package org.ekstep.analytics.model.report

import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.framework.util.{HTTPClient, JSONUtils}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.util._
import org.sunbird.cloud.storage.BaseStorageService

import scala.io.Source

class TestTextbookProgressModel extends SparkSpec(null) with MockFactory{

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession();
    EmbeddedCassandra.loadData("src/test/resources/reports/reports_test_data.cql")
  }

  "TextbookProgressModel" should "execute the model without any error" in {
    val config = """{"search":{"type":"none"},"model":"org.ekstep.analytics.model.report.TextBookProgressModel","modelParams":{"reportConfig":{"id":"tpd_metrics","metrics":[],"labels":{"board":"Board","medium":"Medium","gradeLevel":"Grade","subject":"Subject","resourceType":"Content Type","live":"Live","review":"Review","draft":"Draft","application_ecml":"Created on Diksha","vedio_youtube":"YouTube Content","video_mp4":"Uploaded Videos","application_pdf":"Text Content","application_html":"Uploaded Interactive Content","creator":"Created By","status":"Status"},"output":[{"type":"csv","dims":[]}]},"store":"local","format":"csv","key":"druid-reports/","filePath":"src/test/resources/druid-reports/","container":"test-container","folderPrefix":["slug","reportName"],"sparkCassandraConnectionHost":"localhost","sparkElasticsearchConnectionHost":"localhost"},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"Textbook Progress Metrics Model","deviceMapping":false}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](config).modelParams

    implicit val mockFc = mock[FrameworkContext]
    val mockRestUtil = mock[HTTPClient]

    val mockStorageService = mock[BaseStorageService]
    (mockFc.getStorageService(_: String)).expects("azure").returns(mockStorageService).anyNumberOfTimes();
    (mockStorageService.upload (_: String, _: String, _: String, _: Option[Boolean], _: Option[Int], _: Option[Int], _: Option[Int])).expects(*, *, *, *, *, *, *).returns("").anyNumberOfTimes();
    (mockStorageService.closeContext _).expects().returns().anyNumberOfTimes()

//        Mock for compositeSearch
    val userdata = JSONUtils.deserialize[ContentInformation](Source.fromInputStream
    (getClass.getResourceAsStream("/textbook-report/contentSearchResponse.json")).getLines().mkString)
    val request = s"""{
                     |      "request": {
                     |        "filters": {
                     |           "status": ["Live","Draft","Review","Unlisted"],
                     |          "contentType": ["Resource"],
                     |          "createdFor": "0123653943740170242"
                     |        },
                     |        "fields": ["channel","identifier","board","gradeLevel",
                     |          "medium","subject","status","creator","lastPublishedOn","createdFor",
                     |          "createdOn","pkgVersion","contentType",
                     |          "mimeType","resourceType", "lastSubmittedOn"
                     |        ],
                     |        "limit": 10000,
                     |        "facets": [
                     |          "status"
                     |        ]
                     |      }
                     |    }""".stripMargin

    (mockRestUtil.post[ContentInformation](_: String, _: String, _: Option[Map[String,String]])(_: Manifest[ContentInformation]))
      .expects(Constants.COMPOSITE_SEARCH_URL, request, None, *)
        .returns(userdata)

    val res = TextbookUtils.getContentDataList("0123653943740170242", mockRestUtil)

    TextbookProgressModel.execute(sc.emptyRDD, jobConfig)
  }

}
