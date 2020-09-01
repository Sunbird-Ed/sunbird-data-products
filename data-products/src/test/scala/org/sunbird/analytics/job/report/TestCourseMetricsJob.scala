package org.sunbird.analytics.job.report

import java.io.File
import java.time.{ZoneOffset, ZonedDateTime}

import cats.syntax.either._
import ing.wbaa.druid._
import ing.wbaa.druid.client.DruidClient
import io.circe._
import io.circe.parser._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, MapType, StringType}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}
import org.ekstep.analytics.framework.util.{HadoopFileUtil, JSONUtils}
import org.ekstep.analytics.framework.{DruidQueryModel, FrameworkContext, JobConfig, StorageConfig}
import org.scalamock.scalatest.MockFactory

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Future

case class ESOutput(name: String, id: String, userId: String, completedOn: String, maskedEmail: String, maskedPhone: String, rootOrgName: String, subOrgName: String, startDate: String, courseId: String, lastUpdatedOn: String, batchId: String, completedPercent: String, districtName: String, blockName: String, externalId: String, subOrgUDISECode: String,StateName: String, enrolledOn: String, certificateStatus: String)
case class ESOutputCBatch(id: String, reportUpdatedOn: String, completedCount: String, participantCount: String)
case class BatchReportOutput(`User ID`: String, `School Name`: String, `Mobile Number`: String, `Certificate Status`: String,
                            `Completion Date`: String, `District Name`: String, `User Name`: String, `External ID`: String,
                            `State Name`: String, `Enrolment Date`: String, `Email ID`: String, `Course Progress`: String,
                            `Organisation Name`: String, `Block Name`: String, `School UDISE Code`: String)

class TestCourseMetricsJob extends BaseReportSpec with MockFactory {
  var spark: SparkSession = _
  var courseBatchDF: DataFrame = _
  var userCoursesDF: DataFrame = _
  var userDF: DataFrame = _
  var locationDF: DataFrame = _
  var orgDF: DataFrame = _
  var userOrgDF: DataFrame = _
  var externalIdentityDF: DataFrame = _
  var systemSettingDF: DataFrame = _
  var reporterMock: ReportGenerator = mock[ReportGenerator]
  val sunbirdCoursesKeyspace = "sunbird_courses"
  val sunbirdKeyspace = "sunbird"

  override def beforeAll(): Unit = {

    super.beforeAll()
    spark = getSparkSession()

    courseBatchDF = spark.read.format("com.databricks.spark.csv").option("header", "true")
      .load("src/test/resources/course-metrics-updater/course_batch_data.csv").cache()

    externalIdentityDF = spark.read.format("com.databricks.spark.csv").option("header", "true")
      .load("src/test/resources/course-metrics-updater/user_external_data.csv").cache()

    userCoursesDF = spark.read.format("com.databricks.spark.csv").option("header", "true")
      .load("src/test/resources/course-metrics-updater/user_courses_data.csv").cache()

    userDF = spark.read.json("src/test/resources/course-metrics-updater/user_data.json").cache()

    locationDF = spark.read.format("com.databricks.spark.csv").option("header", "true")
      .load("src/test/resources/course-metrics-updater/location_data.csv").cache()

    orgDF = spark.read.json("src/test/resources/course-metrics-updater/organisation.json").cache()

    userOrgDF = spark.read.format("com.databricks.spark.csv").option("header", "true")
      .load("src/test/resources/course-metrics-updater/user_org_data.csv").cache()

    systemSettingDF = spark.read.format("com.databricks.spark.csv").option("header", "true")
      .load("src/test/resources/course-metrics-updater/system_settings.csv").cache()
  }

  "TestUpdateCourseMetrics" should "generate reports for 10 batches and validate all scenarios" in {

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(courseBatchDF)


    val convertMethod = udf((value: mutable.WrappedArray[String]) => {
      if(null != value && value.nonEmpty)
        value.toList.map(str => JSONUtils.deserialize(str)(manifest[Map[String, String]])).toArray
      else null
    }, new ArrayType(MapType(StringType, StringType), true))

    val alteredUserCourseDf = userCoursesDF.withColumn("certificates", convertMethod(split(userCoursesDF.col("certificates"), ",").cast("array<string>")) )
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_enrolments", "keyspace" -> sunbirdCoursesKeyspace))
      .anyNumberOfTimes()
      .returning(alteredUserCourseDf)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
      .anyNumberOfTimes()
      .returning(userDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace))
      .anyNumberOfTimes()
      .returning(userOrgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
      .anyNumberOfTimes()
      .returning(orgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
      .anyNumberOfTimes()
      .returning(locationDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
      .anyNumberOfTimes()
      .returning(externalIdentityDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "system_settings", "keyspace" -> sunbirdKeyspace))
      .anyNumberOfTimes()
      .returning(systemSettingDF)

    val outputLocation = "/tmp/course-metrics"
    val outputDir = "course-progress-reports"
    val storageConfig = StorageConfig("local", "", outputLocation)

    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    val strConfig= """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.CourseMetricsJob","modelParams":{"druidConfig":{"queryType":"groupBy","dataSource":"content-model-snapshot","intervals":"LastDay","granularity":"all","aggregations":[{"name":"count","type":"count","fieldName":"count"}],"dimensions":[{"fieldName":"identifier","aliasName":"identifier"},{"fieldName":"channel","aliasName":"channel"}],"filters":[{"type":"equals","dimension":"contentType","value":"Course"}],"descending":"false"},"fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')","sparkCassandraConnectionHost":"'$sunbirdPlatformCassandraHost'","sparkElasticsearchConnectionHost":"'$sunbirdPlatformElasticsearchHost'"},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"Course Dashboard Metrics","deviceMapping":false}"""
    val config = JSONUtils.deserialize[JobConfig](strConfig)
    val druidConfig = JSONUtils.deserialize[DruidQueryModel](JSONUtils.serialize(config.modelParams.get("druidConfig")))
    //mocking for DruidDataFetcher
    import scala.concurrent.ExecutionContext.Implicits.global
    val json: String =
      """
        |{
        |  "identifier": "do_1130264512015646721166",
        |  "channel": "01274266675936460840172"
        |}
      """.stripMargin

    val doc: Json = parse(json).getOrElse(Json.Null)
    val results = List(DruidResult.apply(Some(ZonedDateTime.of(2020, 1, 23, 17, 10, 3, 0, ZoneOffset.UTC)), doc));
    val druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.GroupBy)

    implicit val mockDruidConfig: DruidConfig = DruidConfig.DefaultConfig

    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.doQuery[DruidResponse](_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future(druidResponse)).anyNumberOfTimes()
    (mockFc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes()

    CourseMetricsJob.prepareReport(spark, storageConfig, reporterMock.loadData,config)

    implicit val batchReportEncoder: Encoder[BatchReportOutput] = Encoders.product[BatchReportOutput]
    val batch1 = "0130320389509939204"
    val batch2 = "0130293763489873929"
    val batch3 = "01303150537737011211"
    val batch4 = "0130271096968396800"
    val batch5 = "0130404613948538885"

    val batchReportsCount = Option(new File(s"$outputLocation/$outputDir").list)
      .map(_.count(_.endsWith(".csv"))).getOrElse(0)

    batchReportsCount should be (5)

    val batch1Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/$outputDir/report-$batch1.csv").as[BatchReportOutput].collectAsList().asScala
    batch1Results.size should be (1)
    batch1Results.map {res => res.`User ID`}.toList should contain theSameElementsAs List("c4cc494f-04c3-49f3-b3d5-7b1a1984abad")
    batch1Results.map {res => res.`External ID`}.toList should contain theSameElementsAs List(null)
    batch1Results.map {res => res.`School UDISE Code`}.toList should contain theSameElementsAs List(null)
    batch1Results.map {res => res.`School Name`}.toList should contain theSameElementsAs List(null)
    batch1Results.map {res => res.`Block Name`}.toList should contain theSameElementsAs List(null)

    val batch2Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/$outputDir/report-$batch2.csv").as[BatchReportOutput].collectAsList().asScala
    batch2Results.size should be (4)
    batch2Results.map {res => res.`User ID`}.toList should contain theSameElementsAs List("1d8d33b7-f199-48c2-9e89-f9b76fde4591", "1e0e9291-4fae-4550-82b1-72ab16b1165d", "efd3e480-77dc-403b-9331-e253fcf2bceb", "302b7ad6-92c5-4498-aa45-79a08a699ad9")

    val batch3Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/$outputDir/report-$batch3.csv").as[BatchReportOutput].collectAsList().asScala
    batch3Results.size should be (3)
    batch3Results.map {res => res.`User ID`}.toList should contain theSameElementsAs List("14e0dee6-213f-478e-b12e-db59d2ec1b30", "c7ef3848-bbdb-4219-8344-817d5b8103fa", "7ca7e64c-5f6f-49c2-9a03-0830cc8e0088")

    val batch4Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/$outputDir/report-$batch4.csv").as[BatchReportOutput].collectAsList().asScala
    batch4Results.size should be (1)
    batch4Results.map {res => res.`User ID`}.toList should contain theSameElementsAs List("97d81cb6-a348-42f1-91ba-150961a74444")
    batch4Results.map {res => res.`External ID`}.toList should contain theSameElementsAs List("65sboa02")
    batch4Results.map {res => res.`School UDISE Code`}.toList should contain theSameElementsAs List("177vid877")
    batch4Results.map {res => res.`School Name`}.toList should contain theSameElementsAs List("Vidyodhaya")
    batch4Results.map {res => res.`Block Name`}.toList should contain theSameElementsAs List("DEVANAHALLI")

    val batch5Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/$outputDir/report-$batch5.csv").as[BatchReportOutput].collectAsList().asScala
    batch5Results.size should be (2)
    batch5Results.map {res => res.`User ID`}.toList should contain theSameElementsAs List("a5fb2584-f809-4582-811c-563699634905", "bfbf2d79-3c86-4b78-82fd-9d260032b42e")

    /*
    // TODO: Add assertions here
    EmbeddedES.getAllDocuments("cbatchstats-08-07-2018-16-30").foreach(f => {
      f.contains("lastUpdatedOn") should be (true)
    })
    EmbeddedES.getAllDocuments("cbatch").foreach(f => {
      f.contains("reportUpdatedOn") should be (true)
    })
    */

    /*
    val esOutput = JSONUtils.deserialize[ESOutput](EmbeddedES.getAllDocuments("cbatchstats-08-07-2018-16-30").head)
    esOutput.name should be ("Rajesh Kapoor")
    esOutput.id should be ("user012:1002")
    esOutput.batchId should be ("1002")
    esOutput.courseId should be ("do_112726725832507392141")
    esOutput.rootOrgName should be ("MPPS BAYYARAM")
    esOutput.subOrgUDISECode should be ("")
    esOutput.blockName should be ("")
    esOutput.maskedEmail should be ("*****@gmail.com")

  val esOutput_cbatch = JSONUtils.deserialize[ESOutputCBatch](EmbeddedES.getAllDocuments("cbatch").head)
    esOutput_cbatch.id should be ("1004")
    esOutput_cbatch.participantCount should be ("4")
    esOutput_cbatch.completedCount should be ("0")
    */

    new HadoopFileUtil().delete(spark.sparkContext.hadoopConfiguration, outputLocation)
  }

}