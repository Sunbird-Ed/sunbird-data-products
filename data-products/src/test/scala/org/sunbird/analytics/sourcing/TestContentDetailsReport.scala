package org.sunbird.analytics.sourcing

import java.time.{ZoneOffset, ZonedDateTime}
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import cats.syntax.either._
import ing.wbaa.druid.client.DruidClient
import ing.wbaa.druid.{DruidConfig, DruidQuery, DruidResult}
import io.circe.Json
import io.circe.parser.parse
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.ekstep.analytics.framework.util.{HadoopFileUtil, JSONUtils}
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers
import org.sunbird.analytics.util.{EmbeddedPostgresql, SparkSpec}

class TestContentDetailsReport extends SparkSpec with Matchers with MockFactory {
  implicit var spark: SparkSession = _
  val programTable = "program"
  val outputLocation = "src/test/resources/sourcing"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession()
    EmbeddedPostgresql.start()
    EmbeddedPostgresql.createProgramTable()
    //Inserting data into postgres
    EmbeddedPostgresql.execute("INSERT INTO program(program_id,status,startdate,enddate,name) VALUES('program-1','Live','2020-01-10','2025-05-09','Dock-Project')")
  }

  override def afterAll() {
    super.afterAll()
    new HadoopFileUtil().delete(spark.sparkContext.hadoopConfiguration, outputLocation)
    EmbeddedPostgresql.dropTable(programTable)
    EmbeddedPostgresql.close()
  }

  it should "execute generate ContentDetails report" in {
    implicit val sc = spark.sparkContext
    implicit val mockFc = mock[FrameworkContext]

    //mocking for DruidDataFetcher
    // Content Response
    val cJson: String =
      """
        |{
        |    "identifier": "do_1895357",
        |    "name": "content-1",
        |    "unitIdentifiers": "do_9635",
        |    "collectionId": "do_11298391390121984011",
        |    "createdBy": "unknown",
        |    "creator": "0987654334567",
        |    "mimeType": "ecml",
        |    "topic": "Hello Rain",
        |    "learningOutcome": "ENG301",
        |    "bloomsLevel": "Application",
        |    "contentType": "MCQ",
        |    "status": "Draft",
        |    "prevStatus": "Live"
        |}
      """.stripMargin

    // Textbook Response
    val tbJson: String =
      """
        |{
        |    "identifier": "do_11298391390121984011",
        |    "channel": "01309282781705830427",
        |    "primaryCategory": "Exam Question Set",
        |    "objectType": "QuestionSet",
        |    "status": "Live",
        |    "name": "Kayal_Book",
        |    "board":"AP",
        |    "gradeLevel": "Class 8",
        |    "date": "2020-03-25",
        |    "medium": "English",
        |    "subject": "Mathematics",
        |    "acceptedContents": "do_1895357",
        |    "reusedContributions":"do_1895357",
        |    "programId": "program-1"
        |}
      """.stripMargin

    val cDoc: Json = parse(cJson).getOrElse(Json.Null)
    val druidContentResponse = DruidResult.apply(Some(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC)), cDoc)

    val tbDoc: Json = parse(tbJson).getOrElse(Json.Null)
    val druidTextbookResponse = DruidResult.apply(Some(ZonedDateTime.of(2019, 11, 28, 17, 0, 0, 0, ZoneOffset.UTC)), tbDoc)

    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockFc.getDruidRollUpClient _).expects().returns(mockDruidClient).anyNumberOfTimes()
    (mockDruidClient.actorSystem _).expects().returning(ActorSystem("TestQuery")).anyNumberOfTimes()
    (mockDruidClient.doQueryAsStream(_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Source(List(druidContentResponse))).noMoreThanOnce()
    (mockDruidClient.doQueryAsStream(_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Source(List(druidTextbookResponse))).noMoreThanOnce()

    val config = """{"search":{"type":"none"},"model":"org.ekstep.analytics.sourcing.SourcingMetrics","modelParams":{"storageKeyConfig":"azure_storage_key","storageSecretConfig":"azure_storage_secret","tenantId":"01309282781705830427","slug":"test-slug","reportConfig":{"id":"textbook_report","metrics":[],"labels":{"programName":"Project Name","programId":"Project ID","contentId":"Content ID","contentName":"Content Name","mimeType":"MimeType","chapterId":"Folder ID","contentStatus":"Content Status","creator":"Creator Name","createdBy":"CreatedBy ID","date":"Date","identifier":"Collection/Question Set ID","name":"Collection/Question Set Name","medium":"Medium","gradeLevel":"Grade","subject":"Subject","board":"Board","grade":"Grade","chapters":"Chapter Name","status":"Textbook Status","objectType":"Object Type","primaryCategory":"Primary category","topic":"Topic","learningOutcome":"Learning Outcome","addedFromLibrary":"Added from library","contentType":"Content Type"},"output":[{"type":"csv","dims":["identifier","channel","name"],"fileParameters":["id","dims"]},{"type":"json","dims":["identifier","channel","name"],"fileParameters":["id","dims"]}],"reportPath":"src/test/resources/sourcing/"},"contentQuery":{"queryType":"groupBy","dataSource":"vdn-content-model-snapshot","intervals":"1901-01-01T00:00:00+00:00/2101-01-01T00:00:00+00:00","aggregations":[{"name":"count","type":"count"}],"dimensions":[{"fieldName":"identifier","aliasName":"identifier"},{"fieldName":"name","aliasName":"name"},{"fieldName":"unitIdentifiers","aliasName":"unitIdentifiers"},{"fieldName":"collectionId","aliasName":"collectionId"},{"fieldName":"createdBy","aliasName":"createdBy"},{"fieldName":"creator","aliasName":"creator"},{"fieldName":"mimeType","aliasName":"mimeType"},{"fieldName":"topic","aliasName":"topic"},{"fieldName":"learningOutcome","aliasName":"learningOutcome"},{"fieldName":"primaryCategory","aliasName":"contentType"}],"filters":[{"type":"notequals","dimension":"contentType","value":"TextBook"},{"type":"in","dimension":"status","values":["Live"]},{"type":"isnotnull","dimension":"collectionId"}],"postAggregation":[],"descending":"false","limitSpec":{"type":"default","limit":1000000,"columns":[{"dimension":"count","direction":"descending"}]}},"textbookQuery":{"queryType":"groupBy","dataSource":"vdn-content-model-snapshot","intervals":"1901-01-01T00:00:00+00:00/2101-01-01T00:00:00+00:00","aggregations":[{"name":"count","type":"count"}],"dimensions":[{"fieldName":"programId","aliasName":"programId"},{"fieldName":"identifier","aliasName":"identifier"},{"fieldName":"name","aliasName":"name"},{"fieldName":"board","aliasName":"board"},{"fieldName":"medium","aliasName":"medium"},{"fieldName":"gradeLevel","aliasName":"gradeLevel"},{"fieldName":"subject","aliasName":"subject"},{"fieldName":"status","aliasName":"status"},{"fieldName":"acceptedContents","aliasName":"acceptedContents"},{"fieldName":"acceptedContributions","aliasName":"acceptedContributions"},{"fieldName":"rejectedContents","aliasName":"rejectedContents"},{"fieldName":"rejectedContributions","aliasName":"rejectedContributions"},{"fieldName":"primaryCategory","aliasName":"primaryCategory"},{"fieldName":"objectType","aliasName":"objectType"},{"fieldName":"reusedContributions","aliasName":"reusedContributions"}],"filters":[{"type":"in","dimension":"primaryCategory","values":["Digital Textbook","Course","Content Playlist","Question paper","Question Paper","Exam Question Set","Practice Set","Demo Practice Question Set"]},{"type":"isnotnull","dimension":"programId"},{"type":"in","dimension":"status","values":["Draft"]},{"type":"equals","dimension":"channel","value":"channelId"}],"postAggregation":[],"descending":"false","limitSpec":{"type":"default","limit":1000000,"columns":[{"dimension":"count","direction":"descending"}]}},"store":"local","storageContainer":"'$reportPostContainer'","format":"csv","key":"druid-reports/","filePath":"druid-reports/","container":"'$reportPostContainer'","sparkCassandraConnectionHost":"'$sunbirdPlatformCassandraHost'","folderPrefix":["slug","reportName"]},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"Textbook Report Job","deviceMapping":false}""".stripMargin
    implicit val jobConfig = JSONUtils.deserialize[JobConfig](config)
    ContentDetailsReport.execute()

    val resultDf = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/test-slug/ContentDetailsReport.csv")

    resultDf.count() should be (1)

    val content = resultDf.collect.map(r => Map(resultDf.columns.zip(r.toSeq):_*)).head

    content("Collection/Question Set ID") should be ("do_11298391390121984011")
    content("Primary category") should be ("Exam Question Set")
    content("Learning Outcome") should be ("ENG301")
    content("Collection/Question Set Name") should be ("Kayal_Book")
    content("Creator Name") should be ("0987654334567")
    content("Board") should be ("AP")
    content("Subject") should be ("Mathematics")
    content("Topic") should be ("Hello Rain")
    content("Content ID") should be ("do_1895357")
    content("Folder ID") should be ("do_9635")
    content("Added from library") should be ("Yes")
    content("Grade") should be ("Class 8")
    content("Medium") should be ("English")
    content("Content Status") should be ("Approved")
    content("Content Name") should be ("content-1")
    content("Content Type") should be ("MCQ")
    content("Project Name") should be ("Dock-Project")
    content("Project ID") should be ("program-1")
    content("Object Type") should be ("QuestionSet")
    content("CreatedBy ID") should be ("unknown")
    content("MimeType") should be ("ecml")
  }
}
