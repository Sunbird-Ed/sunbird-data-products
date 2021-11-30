package org.sunbird.analytics.sourcing

import java.time.{ZoneOffset, ZonedDateTime}

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import cats.syntax.either._
import ing.wbaa.druid.client.DruidClient
import ing.wbaa.druid._
import io.circe.Json
import io.circe.parser.parse
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Encoders, SQLContext, SparkSession}
import org.ekstep.analytics.framework.util.{HadoopFileUtil, JSONUtils}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig, StorageConfig}
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers
import org.sunbird.analytics
import org.sunbird.analytics.sourcing.SourcingMetrics.sunbirdHierarchyStore
import org.sunbird.analytics.util.{EmbeddedCassandra, SparkSpec}

import scala.concurrent.Future

class TestSourcingMetrics extends SparkSpec with Matchers with MockFactory {
  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession()
    EmbeddedCassandra.loadData("src/test/resources/reports/reports_textbook_data.cql")
  }

  override def afterAll() : Unit = {
    super.afterAll()
    new HadoopFileUtil().delete(spark.sparkContext.hadoopConfiguration, "sourcing")
  }

  it should "execute generate SourcingMetrics report" in {
    implicit val sc = spark.sparkContext
    implicit val mockFc = mock[FrameworkContext]
    val sqlContext = new SQLContext(sc)

    //mocking for DruidDataFetcher
    import scala.concurrent.ExecutionContext.Implicits.global
    val json: String =
      """
        |{
        |    "identifier": "do_11298391390121984011",
        |    "channel": "0124698765480987654",
        |    "status": "Live",
        |    "name": "Kayal_Book",
        |    "board":"AP",
        |    "gradeLevel": "Class 8",
        |    "date": "2020-03-25",
        |    "medium": "English",
        |    "subject": "Mathematics",
        |    "primaryCategory": "Digital Textbook"
        |  }
      """.stripMargin

    val doc: Json = parse(json).getOrElse(Json.Null)
    val results = List(DruidResult.apply(Some(ZonedDateTime.of(2020, 1, 23, 17, 10, 3, 0, ZoneOffset.UTC)), doc))
    val druidResponse = DruidResult.apply(Some(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC)), doc)
    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.actorSystem _).expects().returning(ActorSystem("TestQuery")).anyNumberOfTimes()
    (mockDruidClient.doQueryAsStream(_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Source(List(druidResponse))).anyNumberOfTimes()
    (mockFc.getDruidRollUpClient _).expects().returns(mockDruidClient).anyNumberOfTimes()
    val config = """{"search": {"type": "none"},"model": "org.ekstep.analytics.job.report.VDNMetricsJob","modelParams": {"reportConfig": {"id": "vdn_report","metrics": [],"labels": {"date": "Date","name": "Textbook Name","medium": "Medium","gradeLevel": "Grade","subject": "Subject","board": "Board","grade": "Grade","chapters": "Chapter Name","totalChapters": "Total number of chapters (first level sections of ToC)","status": "Textbook Status"},"output": [{"type": "csv","dims": ["identifier", "channel", "name"],"fileParameters": ["id", "dims"]}, {"type": "json","dims": ["identifier", "channel", "name"],"fileParameters": ["id", "dims"]}]},"druidConfig": {"queryType": "groupBy","dataSource": "content-model-snapshot","intervals": "1901-01-01T00:00:00+00:00/2101-01-01T00:00:00+00:00","aggregations": [{"name": "count","type": "count"}],"dimensions": [{"fieldName": "channel","aliasName": "channel"}, {"fieldName": "identifier","aliasName": "identifier","type": "Extraction","outputType": "STRING","extractionFn": [{"type": "javascript","fn": "function(str){return str == null ? null: str.split(\".\")[0]}"}]}, {"fieldName": "name","aliasName": "name"}, {"fieldName": "createdFor","aliasName": "createdFor"}, {"fieldName": "createdOn","aliasName": "createdOn"}, {"fieldName": "lastUpdatedOn","aliasName": "lastUpdatedOn"}, {"fieldName": "board","aliasName": "board"}, {"fieldName": "medium","aliasName": "medium"}, {"fieldName": "gradeLevel","aliasName": "gradeLevel"}, {"fieldName": "subject","aliasName": "subject"}, {"fieldName": "status","aliasName": "status"}],"filters": [{"type": "equals","dimension": "contentType","value": "TextBook"}, {"type": "equals","dimension": "channel","value": "01306475770753843226"}, {"type": "in","dimension": "status","values": ["Live", "Draft", "Review"]}],"postAggregation": [],"descending": "false","limitSpec": {"type": "default","limit": 1000000,"columns": [{"dimension": "count","direction": "descending"}]}},"store": "local","format": "csv","key": "druid-reports/","filePath": "druid-reports/","container": "test-container","folderPrefix": ["slug", "reportName"]},"output": [{"to": "console","params": {"printEvent": false}}],"parallelization": 8,"appName": "VDN Metrics Job","deviceMapping": false}""".stripMargin
    implicit val jobConfig = JSONUtils.deserialize[JobConfig](config)

    SourcingMetrics.execute()

    val chapterReport = sqlContext.sparkSession.read
      .option("header","true")
      .csv("sourcing/Unknown/FolderLevel.csv")
    chapterReport.first().getString(0) should be("AP") //Assertion for board
    chapterReport.first().getString(1) should be("English") //Assertion for medium
    chapterReport.first().getString(2) should be("Class 8") //Assertion for grade
    chapterReport.first().getString(3) should be("Mathematics") //Assertion for subject
    chapterReport.first().getString(4) should be("Kayal_Book") //Assertion for textbook name
    chapterReport.first().getString(5) should be("Test_TextBook_name_8636893549") //Assertion for chaptername

    val textbookReport = sqlContext.sparkSession.read
      .option("header","true")
      .csv("sourcing/Unknown/CollectionLevel.csv")
    textbookReport.first().getString(0) should be("AP") //Assertion for board
    textbookReport.first().getString(1) should be("English") //Assertion for medium
    textbookReport.first().getString(2) should be("Class 8") //Assertion for grade
    textbookReport.first().getString(3) should be("Mathematics") //Assertion for subject
    textbookReport.first().getString(4) should be("Kayal_Book") //Assertion for textbook name
    textbookReport.first().getString(5) should be("1") //Assertion for total number of chapters in textbook

  }

  ignore should "generate hierarchy report" in {
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val config = """{"search": {"type": "none"},"druidConfig": {"queryType": "groupBy","dataSource": "content-model-snapshot","intervals": "1901-01-01T00:00:00+00:00/2101-01-01T00:00:00+00:00","aggregations": [{"name": "count","type": "count"}],"dimensions": [{"fieldName": "channel","aliasName": "channel"}, {"fieldName": "identifier","aliasName": "identifier","type": "Extraction","outputType": "STRING","extractionFn": [{"type": "javascript","fn": "function(str){return str == null ? null: str.split(\".\")[0]}"}]}, {"fieldName": "name","aliasName": "name"}, {"fieldName": "createdFor","aliasName": "createdFor"}, {"fieldName": "createdOn","aliasName": "createdOn"}, {"fieldName": "lastUpdatedOn","aliasName": "lastUpdatedOn"}, {"fieldName": "board","aliasName": "board"}, {"fieldName": "medium","aliasName": "medium"}, {"fieldName": "gradeLevel","aliasName": "gradeLevel"}, {"fieldName": "subject","aliasName": "subject"}, {"fieldName": "status","aliasName": "status"}],"filters": [{"type": "equals","dimension": "contentType","value": "TextBook"}, {"type": "equals","dimension": "channel","value": "01306475770753843226"}, {"type": "in","dimension": "status","values": ["Live", "Draft", "Review"]}],"postAggregation": [],"descending": "false","limitSpec": {"type": "default","limit": 1000000,"columns": [{"dimension": "count","direction": "descending"}]}},"model": "org.ekstep.analytics.job.report.VDNMetricsJob","modelParams": {"reportConfig": {"id": "vdn_report","metrics": [],"labels": {"date": "Date","name": "Textbook Name","medium": "Medium","gradeLevel": "Grade","subject": "Subject","board": "Board","grade": "Grade","chapters": "Chapter Name","totalChapters": "Total number of chapters (first level sections of ToC)","status": "Textbook Status"},"output": [{"type": "csv","dims": ["identifier", "channel", "name"],"fileParameters": ["id", "dims"]}]},"store": "local","format": "csv","key": "druid-reports/","filePath": "druid-reports/","container": "test-container","folderPrefix": ["slug", "reportName"]},"output": [{"to": "console","params": {"printEvent": false}}],"parallelization": 8,"appName": "VDN Metrics Job","deviceMapping": false}""".stripMargin
    val encoders = Encoders.product[ContentHierarchy]
    val data = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "content_hierarchy", "keyspace" -> sunbirdHierarchyStore)).load()
      .as[ContentHierarchy](encoders).first()
    val hierarchy = JSONUtils.deserialize[TextbookHierarchy](data.hierarchy)
    val report = SourcingMetrics.generateReport(List(hierarchy),List(), List(),hierarchy,List(),List("","0"))
    val df = report._1.toDF().withColumn("slug",lit("test-slug")).withColumn("reportName",lit("TextbookReport"))
    val configMap = JSONUtils.deserialize[Map[String,AnyRef]](config)
    val reportConfig = configMap("modelParams").asInstanceOf[Map[String, AnyRef]]("reportConfig").asInstanceOf[Map[String, AnyRef]]
    SourcingMetrics.saveReportToBlob(df,JSONUtils.serialize(reportConfig),StorageConfig("local","test","Textbook",Option(""),Option("")),"TextbookReport", "sourcing")
    val level1identifiers = List("do_1130548235422269441989")
    val contentTypes = List("Resource","TextBook")

    report._1.length should be (1)
    report._1.map(f => {
      level1identifiers.contains(f.l1identifier) should be (true)
      f.channel should be ("channel-01")
      f.name should be ("KP Integration Test Collection Content")
      f.chapters should be ("Test_TextBook_name_8636893549")
    })

    report._2.length should be (2)
    report._2.map(f => {
      f.contentType should not be("TextbookUnit")
      level1identifiers.contains(f.l1identifier) should be (true)
      f.identifier should be ("KP_FT_1593606389382")
    })
    report._2.map(f=>f.contentType) should equal(contentTypes)
  }

}
