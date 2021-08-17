package org.sunbird.analytics.sourcing

import java.time.{ZoneOffset, ZonedDateTime}

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import cats.syntax.either._
import ing.wbaa.druid.client.DruidClient
import ing.wbaa.druid.{DruidConfig, DruidQuery, DruidResponse, DruidResponseTimeseriesImpl, DruidResult, QueryType}
import io.circe.Json
import io.circe.parser.parse
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.{Encoders, SQLContext, SparkSession}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.ekstep.analytics.framework.util.JSONUtils
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers
import org.sunbird.analytics.util.{EmbeddedPostgresql, SparkSpec}

import scala.concurrent.Future
case class TestContentDetails(contentId: String, contentName: String, primaryCategory: String, identifier: String, name: String, board: String, medium: String, gradeLevel: String, subject: String, programId: String, createdBy: String, creator: String, mimeType: String, unitIdentifiers: String, status: String, prevStatus: String, acceptedContents: String, rejectedContents: String)

class TestContentDetailsReport extends SparkSpec with Matchers with MockFactory {
  implicit var spark: SparkSession = _
  val programTable = "program"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession()
    EmbeddedPostgresql.start()
    EmbeddedPostgresql.createProgramTable()
  }

  override def afterAll() {
    super.afterAll()
    EmbeddedPostgresql.dropTable(programTable)
    EmbeddedPostgresql.close()
  }

  it should "execute generate ContentDetails report" in {
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
        |    "primaryCategory": "digital textbook"
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
    val config = """{"search": {"type": "none"},"model": "org.ekstep.analytics.sourcing.SourcingMetrics","modelParams": {"storageKeyConfig": "azure_storage_key","storageSecretConfig": "azure_storage_secret","tenantId": "01309282781705830427","slug": "test-slug","reportConfig": {"id": "textbook_report","metrics": [],"labels": {"programName": "Project Name","programId": "Project ID","contentId": "Content ID","contentName": "Content Name","contentType": "Content Type","mimeType": "MimeType","chapterId": "Chapter ID","contentStatus": "Content Status","creator": "Creator Name","createdBy": "CreatedBy ID","date": "Date","identifier": "Textbook ID","name": "Textbook Name","medium": "Medium","gradeLevel": "Grade","subject": "Subject","createdOn": "Created On","lastUpdatedOn": "Last Updated On","reportDate": "Report generation date","board": "Board","grade": "Grade","chapters": "Chapter Name","totalChapters": "Total number of chapters (first level sections of ToC)","status": "Textbook Status"},"output": [{"type": "csv","dims": ["identifier", "channel", "name"],"fileParameters": ["id", "dims"]}, {"type": "json","dims": ["identifier", "channel", "name"],"fileParameters": ["id", "dims"]}]},"contentQuery": {"queryType": "groupBy","dataSource": "vdn-content-model-snapshot","intervals": "1901-01-01T00:00:00+00:00/2101-01-01T00:00:00+00:00","aggregations": [{"name": "count","type": "count"}],"dimensions": [{"fieldName": "identifier","aliasName": "identifier"}, {"fieldName": "name","aliasName": "name"}, {"fieldName": "primaryCategory","aliasName": "primaryCategory"}, {"fieldName": "unitIdentifiers","aliasName": "unitIdentifiers"}, {"fieldName": "collectionId","aliasName": "collectionId"}, {"fieldName": "createdBy","aliasName": "createdBy"}, {"fieldName": "creator","aliasName": "creator"}, {"fieldName": "mimeType","aliasName": "mimeType"}],"filters": [{"type": "notequals","dimension": "contentType","value": "TextBook"}, {"type": "in","dimension": "status","values": ["Live"]}, {"type": "isnotnull","dimension": "collectionId"}],"postAggregation": [],"descending": "false","limitSpec": {"type": "default","limit": 1000000,"columns": [{"dimension": "count","direction": "descending"}]}},"textbookQuery": {"queryType": "groupBy","dataSource": "vdn-content-model-snapshot","intervals": "1901-01-01T00:00:00+00:00/2101-01-01T00:00:00+00:00","aggregations": [{"name": "count","type": "count"}],"dimensions": [{"fieldName": "programId","aliasName": "programId"}, {"fieldName": "identifier","aliasName": "identifier"}, {"fieldName": "name","aliasName": "name"}, {"fieldName": "board","aliasName": "board"}, {"fieldName": "medium","aliasName": "medium"}, {"fieldName": "gradeLevel","aliasName": "gradeLevel"}, {"fieldName": "subject","aliasName": "subject"}, {"fieldName": "status","aliasName": "status"}, {"fieldName": "acceptedContents","aliasName": "acceptedContents"}, {"fieldName": "rejectedContents","aliasName": "rejectedContents"}],"filters": [{"type": "in","dimension": "contentType","values": ["TextBook", "Course", "Collection"]}, {"type": "isnotnull","dimension": "programId"}, {"type": "in","dimension": "status","values": ["Draft"]}, {"type": "equals","dimension": "channel","value": "channelId"}],"postAggregation": [],"descending": "false","limitSpec": {"type": "default","limit": 1000000,"columns": [{"dimension": "count","direction": "descending"}]}},"store": "local","storageContainer": "'$reportPostContainer'","format": "csv","key": "druid-reports/","filePath": "druid-reports/","container": "'$reportPostContainer'","sparkCassandraConnectionHost": "'$sunbirdPlatformCassandraHost'","folderPrefix": ["slug", "reportName"]},"output": [{"to": "console","params": {"printEvent": false}}],"parallelization": 8,"appName": "Textbook Report Job","deviceMapping": false}""".stripMargin
    implicit val jobConfig = JSONUtils.deserialize[JobConfig](config)
    ContentDetailsReport.execute()

  }

  it should "get content status for all contents" in {
    implicit val sc = spark.sparkContext
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    implicit val mockFc = mock[FrameworkContext]
    val reportDf = List(TestContentDetails("do_1895357","book-1","eTextbook","do_9635","tb-01","ncrt","english","class 6","maths",
    "program-1","unknown","0987654334567","ecml","do_952457879895","live","draft","do_898765545","do_87654")).toDF()
      .groupBy("contentId","contentName","primaryCategory","identifier",
        "name","board","medium","gradeLevel","subject","programId","createdBy",
        "creator","mimeType","unitIdentifiers", "status","prevStatus")
      .agg(collect_list("acceptedContents").as("acceptedContents"),collect_list("rejectedContents").as("rejectedContents"))

    //Inserting data into postgres
    EmbeddedPostgresql.execute("INSERT INTO program(program_id,status,startdate,enddate,name) VALUES('program-1','Live','2020-01-10','2025-05-09','Dock-Project')")

    val df = ContentDetailsReport.getContentDetails(reportDf,"test-slug")
    val encoder = Encoders.product[ContentReport]
    val data = df.as[ContentReport](encoder).rdd
    data.collect().map(f => {
      f.contentType should be ("eTextbook")
      f.identifier should be ("do_9635")
    })
  }

}
