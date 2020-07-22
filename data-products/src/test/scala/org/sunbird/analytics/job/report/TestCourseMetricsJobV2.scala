package org.sunbird.analytics.job.report

import java.io.File

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, MapType, StringType}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}
import org.ekstep.analytics.framework.util.{HadoopFileUtil, JSONUtils}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig, StorageConfig}
import org.scalamock.scalatest.MockFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

class TestCourseMetricsJobV2 extends BaseReportSpec with MockFactory with BaseReportsJob {
  var spark: SparkSession = _
  var courseBatchDF: DataFrame = _
  var userCoursesDF: DataFrame = _
  var userDF: DataFrame = _
  var reporterMock: ReportGeneratorV2 = mock[ReportGeneratorV2]
  val sunbirdCoursesKeyspace = "sunbird_courses"

  override def beforeAll(): Unit = {

    super.beforeAll()
    spark = getSparkSession()

    courseBatchDF = spark.read.format("com.databricks.spark.csv").option("header", "true")
      .load("src/test/resources/course-metrics-updaterv2/course_batch_data.csv").cache()

    userCoursesDF = spark.read.format("com.databricks.spark.csv").option("header", "true")
      .load("src/test/resources/course-metrics-updaterv2/user_courses_data.csv").cache()

    userDF = spark.read.json("src/test/resources/course-metrics-updaterv2/user_data.json").cache()
  }

  "TestUpdateCourseMetricsV2" should "generate reports for batches and validate all scenarios" in {
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra")
      .returning(courseBatchDF)

    (reporterMock.loadData _)
      .expects(spark, Map("keys.pattern" -> "*","infer.schema" -> "true"),"org.apache.spark.sql.redis")
      .anyNumberOfTimes()
      .returning(userDF)

    CourseMetricsJobV2.loadData(spark, Map("table" -> "user", "keyspace" -> "sunbird"),"org.apache.spark.sql.cassandra")


    val convertMethod = udf((value: mutable.WrappedArray[String]) => {
      if(null != value && value.nonEmpty)
        value.toList.map(str => JSONUtils.deserialize(str)(manifest[Map[String, String]])).toArray
      else null
    }, new ArrayType(MapType(StringType, StringType), true))

    val alteredUserCourseDf = userCoursesDF.withColumn("certificates", convertMethod(split(userCoursesDF.col("certificates"), ",").cast("array<string>")) )
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra")
      .anyNumberOfTimes()
      .returning(alteredUserCourseDf)

    val outputLocation = "/tmp/course-metrics"
    val outputDir = "course-progress-reports"
    val storageConfig = StorageConfig("local", "", outputLocation)

    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    val strConfig= """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.CourseMetricsJobV2","modelParams":{"batchFilters":["TPD"],"druidConfig":{"queryType":"groupBy","dataSource":"content-model-snapshot","intervals":"LastDay","granularity":"all","aggregations":[{"name":"count","type":"count","fieldName":"count"}],"dimensions":[{"fieldName":"identifier","aliasName":"identifier"},{"fieldName":"channel","aliasName":"channel"}],"filters":[{"type":"equals","dimension":"contentType","value":"Course"}],"descending":"false"},"fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')","sparkCassandraConnectionHost":"'$sunbirdPlatformCassandraHost'","sparkElasticsearchConnectionHost":"'$sunbirdPlatformElasticsearchHost'"},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"Course Dashboard Metrics","deviceMapping":false}"""
    val config = JSONUtils.deserialize[JobConfig](strConfig)
    val reportId: String = config.modelParams.getOrElse(Map[String, AnyRef]()).getOrElse("reportId", "").asInstanceOf[String]

    CourseMetricsJobV2.prepareReport(spark, storageConfig, reporterMock.loadData, config, List(), reportId)

    implicit val batchReportEncoder: Encoder[BatchReportOutput] = Encoders.product[BatchReportOutput]
    val batch1 = "01303150537737011211"
    val batch2 = "0130334873750159361"

    val batchReportsCount = Option(new File(s"$outputLocation/$outputDir").list)
      .map(_.count(_.endsWith(".csv"))).getOrElse(0)

    batchReportsCount should be (2)

    val batch1Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/$outputDir/report-$batch1.csv").as[BatchReportOutput].collectAsList().asScala
    batch1Results.map {res => res.`User ID`}.toList should contain theSameElementsAs List("c7ef3848-bbdb-4219-8344-817d5b8103fa")
//    batch1Results.map {res => res.`External ID`}.toList should contain theSameElementsAs List(null)
//    batch1Results.map {res => res.`School UDISE Code`}.toList should contain theSameElementsAs List(null)
//    batch1Results.map {res => res.`School Name`}.toList should contain theSameElementsAs List(null)
//    batch1Results.map {res => res.`Block Name`}.toList should contain theSameElementsAs List(null)

    val batch2Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/$outputDir/report-$batch2.csv").as[BatchReportOutput].collectAsList().asScala
    batch2Results.map {res => res.`User ID`}.toList should contain theSameElementsAs List("f3dd58a4-a56f-4c1d-95cf-3231927a28e9")
//    batch2Results.map {res => res.`External ID`}.toList should contain theSameElementsAs List(null)
//    batch2Results.map {res => res.`School UDISE Code`}.toList should contain theSameElementsAs List(null)
//    batch2Results.map {res => res.`School Name`}.toList should contain theSameElementsAs List(null)
//    batch2Results.map {res => res.`Block Name`}.toList should contain theSameElementsAs List(null)
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

  it should "generate reports for batches and validate all scenarios for NISHTHA reports" in {
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra")
      .returning(courseBatchDF)

    (reporterMock.loadData _)
      .expects(spark, Map("keys.pattern" -> "*","infer.schema" -> "true"),"org.apache.spark.sql.redis")
      .anyNumberOfTimes()
      .returning(userDF)

    CourseMetricsJobV2.loadData(spark, Map("table" -> "user", "keyspace" -> "sunbird"),"org.apache.spark.sql.cassandra")


    val convertMethod = udf((value: mutable.WrappedArray[String]) => {
      if(null != value && value.nonEmpty)
        value.toList.map(str => JSONUtils.deserialize(str)(manifest[Map[String, String]])).toArray
      else null
    }, new ArrayType(MapType(StringType, StringType), true))

    val alteredUserCourseDf = userCoursesDF.withColumn("certificates", convertMethod(split(userCoursesDF.col("certificates"), ",").cast("array<string>")) )
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra")
      .anyNumberOfTimes()
      .returning(alteredUserCourseDf)

    val outputLocation = "/tmp/course-metrics"
    val outputDir = "course-progress-reports"
    val storageConfig = StorageConfig("local", "", outputLocation)

    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    val strConfig= """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.CourseMetricsJobV2","modelParams":{"reportId": "NISHTHA-reports", "batchFilters":["TPD"],"druidConfig":{"queryType":"groupBy","dataSource":"content-model-snapshot","intervals":"LastDay","granularity":"all","aggregations":[{"name":"count","type":"count","fieldName":"count"}],"dimensions":[{"fieldName":"identifier","aliasName":"identifier"},{"fieldName":"channel","aliasName":"channel"}],"filters":[{"type":"equals","dimension":"contentType","value":"Course"}],"descending":"false"},"fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')","sparkCassandraConnectionHost":"'$sunbirdPlatformCassandraHost'","sparkElasticsearchConnectionHost":"'$sunbirdPlatformElasticsearchHost'"},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"Course Dashboard Metrics","deviceMapping":false}"""
    val config = JSONUtils.deserialize[JobConfig](strConfig)
    val reportId: String = config.modelParams.getOrElse(Map[String, AnyRef]()).getOrElse("reportId", "").asInstanceOf[String]

    CourseMetricsJobV2.prepareReport(spark, storageConfig, reporterMock.loadData, config, List(), reportId)

    implicit val batchReportEncoder: Encoder[BatchReportOutput] = Encoders.product[BatchReportOutput]
    val batch1 = "01303150537737011211"
    val batch2 = "0130334873750159361"

    val batchReportsCount = Option(new File(s"$outputLocation/$outputDir").list)
      .map(_.count(_.endsWith(".csv"))).getOrElse(0)

    batchReportsCount should be (2)

    val batch1Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/$outputDir/report-$batch1.csv").as[BatchReportOutput].collectAsList().asScala
    batch1Results.map {res => res.`User ID`}.toList should contain theSameElementsAs List("c7ef3848-bbdb-4219-8344-817d5b8103fa")
//    batch1Results.map {res => res.`External ID`}.toList should contain theSameElementsAs List(null)
//    batch1Results.map {res => res.`School UDISE Code`}.toList should contain theSameElementsAs List(null)
//    batch1Results.map {res => res.`School Name`}.toList should contain theSameElementsAs List(null)
//    batch1Results.map {res => res.`Block Name`}.toList should contain theSameElementsAs List(null)

    val batch2Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/$outputDir/report-$batch2.csv").as[BatchReportOutput].collectAsList().asScala
    batch2Results.map {res => res.`User ID`}.toList should contain theSameElementsAs List("f3dd58a4-a56f-4c1d-95cf-3231927a28e9")
//    batch2Results.map {res => res.`External ID`}.toList should contain theSameElementsAs List(null)
//    batch2Results.map {res => res.`School UDISE Code`}.toList should contain theSameElementsAs List(null)
//    batch2Results.map {res => res.`School Name`}.toList should contain theSameElementsAs List(null)
//    batch2Results.map {res => res.`Block Name`}.toList should contain theSameElementsAs List(null)
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

  it should "test redis and cassandra connections" in {
    implicit val fc = Option(mock[FrameworkContext])
    spark.sparkContext.stop()

    val strConfig = """{"search": {"type": "none"},"model": "org.sunbird.analytics.job.report.CourseMetricsJob","modelParams": {"batchFilters": ["TPD"],"fromDate": "$(date --date yesterday '+%Y-%m-%d')","toDate": "$(date --date yesterday '+%Y-%m-%d')","sparkCassandraConnectionHost": "127.0.0.0","sparkElasticsearchConnectionHost": "'$sunbirdPlatformElasticsearchHost'","sparkRedisConnectionHost": "'$sparkRedisConnectionHost'","sparkUserDbRedisIndex": "4"},"output": [{"to": "console","params": {"printEvent": false}}],"parallelization": 8,"appName": "Course Dashboard Metrics","deviceMapping": false}""".stripMargin
    getReportingSparkContext(JSONUtils.deserialize[JobConfig](strConfig))
    val conf = openSparkSession(JSONUtils.deserialize[JobConfig](strConfig))
    conf.sparkContext.stop()
    spark = getSparkSession()
  }

}