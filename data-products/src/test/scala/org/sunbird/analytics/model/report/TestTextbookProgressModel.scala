package org.ekstep.analytics.model.report

import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.util.{EmbeddedCassandra, SparkSpec}

class TestTextbookProgressModel extends SparkSpec(null) with MockFactory{

  implicit val fc = new FrameworkContext()
  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession();
    EmbeddedCassandra.loadData("src/test/resources/reports/reports_test_data.cql")
  }

  "TextbookProgressModel" should "execute the model without any error" in {
    val config = """{"search":{"type":"none"},"model":"org.ekstep.analytics.model.report.TextBookProgressModel","modelParams":{"reportConfig":{"id":"tpd_metrics","metrics":[],"labels":{"board":"Board","medium":"Medium","gradeLevel":"Grade","subject":"Subject","resourceType":"Content Type","live":"Live","review":"Review","draft":"Draft","application_ecml":"Created on Diksha","vedio_youtube":"YouTube Content","video_mp4":"Uploaded Videos","application_pdf":"Text Content","application_html":"Uploaded Interactive Content","creator":"Created By","status":"Status"},"output":[{"type":"csv","dims":[]}]},"store":"local","format":"csv","key":"druid-reports/","filePath":"src/test/resources/druid-reports/","container":"test-container","folderPrefix":["slug","reportName"],"sparkCassandraConnectionHost":"localhost","sparkElasticsearchConnectionHost":"localhost"},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"Textbook Progress Metrics Model","deviceMapping":false}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](config).modelParams

//    Mock for compositeSearch
//    val userdata = JSONUtils.deserialize[ContentInfo](Source.fromInputStream
//    (getClass.getResourceAsStream("/textbook-report/contentSearchResponse.json")).getLines().mkString)

//    println("userdata: " + userdata)

    TextbookProgressModel.execute(sc.emptyRDD, jobConfig)
  }

}
