package org.sunbird.analytics.job.report

import java.io.File

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}
import org.ekstep.analytics.framework.util.{HadoopFileUtil, JSONUtils}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig, StorageConfig}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.util.UserData

import scala.collection.JavaConverters._
import scala.collection.mutable

class TestCourseMetricsJobV2 extends BaseReportSpec with MockFactory with BaseReportsJob {
  var spark: SparkSession = _
  var courseBatchDF: DataFrame = _
  var userCoursesDF: DataFrame = _
  var userDF: DataFrame = _
  var reporterMock: ReportGeneratorV2 = mock[ReportGeneratorV2]
  val sunbirdCoursesKeyspace = "sunbird_courses"
  val sunbirdKeyspace = "sunbird"

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
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra", new StructType())
      .returning(courseBatchDF)

    val schema = Encoders.product[UserData].schema
    (reporterMock.loadData _)
      .expects(spark, Map("keys.pattern" -> "*","infer.schema" -> "true"),"org.apache.spark.sql.redis", schema)
      .anyNumberOfTimes()
      .returning(userDF)

    CourseMetricsJobV2.loadData(spark, Map("table" -> "user", "keyspace" -> "sunbird"),"org.apache.spark.sql.cassandra", new StructType())


    val convertMethod = udf((value: mutable.WrappedArray[String]) => {
      if(null != value && value.nonEmpty)
        value.toList.map(str => JSONUtils.deserialize(str)(manifest[Map[String, String]])).toArray
      else null
    }, new ArrayType(MapType(StringType, StringType), true))

    val alteredUserCourseDf = userCoursesDF.withColumn("certificates", convertMethod(split(userCoursesDF.col("certificates"), ",").cast("array<string>")) )
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra", new StructType())
      .anyNumberOfTimes()
      .returning(alteredUserCourseDf)

    val outputLocation = "/tmp/course-metrics"
    val outputDir = "course-progress-reports"
    val storageConfig = StorageConfig("local", "", outputLocation)

    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    val strConfig= """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.CourseMetricsJobV2","modelParams":{"batchFilters":["TPD"],"fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')","sparkCassandraConnectionHost":"'$sunbirdPlatformCassandraHost'","sparkElasticsearchConnectionHost":"'$sunbirdPlatformElasticsearchHost'","sparkRedisConnectionHost":"'$sparkRedisConnectionHost'","sparkUserDbRedisIndex":"12"},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"Course Dashboard Metrics","deviceMapping":false}"""
    val config = JSONUtils.deserialize[JobConfig](strConfig)
//    CourseMetricsJobV2.main(strConfig)
    CourseMetricsJobV2.prepareReport(spark, storageConfig, reporterMock.loadData, config, List())

    implicit val sc = spark

    val batchInfo = List(CourseBatch("01303150537737011211","2020-05-29","2030-06-30","b00bc992ef25f1a9a8d63291e20efc8d"), CourseBatch("0130334873750159361","2020-06-11","2030-06-30","013016492159606784174"))
    batchInfo.map(batches => {
      val reportDf = CourseMetricsJobV2.getReportDF(batches,userDF,reporterMock.loadData, config)
      CourseMetricsJobV2.saveReportToBlobStore(batches, reportDf, storageConfig, reportDf.count())
    })

    implicit val batchReportEncoder: Encoder[BatchReportOutput] = Encoders.product[BatchReportOutput]
    val batch1 = "01303150537737011211"
    val batch2 = "0130334873750159361"

    val batchReportsCount = Option(new File(s"$outputLocation/$outputDir").list)
      .map(_.count(_.endsWith(".csv"))).getOrElse(0)

    batchReportsCount should be (2)

    val batch1Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/$outputDir/report-$batch1.csv").as[BatchReportOutput].collectAsList().asScala
    batch1Results.map {res => res.`User ID`}.toList should contain theSameElementsAs List("c7ef3848-bbdb-4219-8344-817d5b8103fa")
    batch1Results.map {res => res.`External ID`}.toList should contain theSameElementsAs List(null)
    batch1Results.map {res => res.`School UDISE Code`}.toList should contain theSameElementsAs List(null)
    batch1Results.map {res => res.`School Name`}.toList should contain theSameElementsAs List(null)
    batch1Results.map {res => res.`Block Name`}.toList should contain theSameElementsAs List(null)

    val batch2Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/$outputDir/report-$batch2.csv").as[BatchReportOutput].collectAsList().asScala
    batch2Results.map {res => res.`User ID`}.toList should contain theSameElementsAs List("f3dd58a4-a56f-4c1d-95cf-3231927a28e9")
    batch2Results.map {res => res.`External ID`}.toList should contain theSameElementsAs List("df09619-fdcvbn")
    batch2Results.map {res => res.`School UDISE Code`}.toList should contain theSameElementsAs List("10")
    batch2Results.map {res => res.`School Name`}.toList should contain theSameElementsAs List("School-2")
    batch2Results.map {res => res.`Block Name`}.toList should contain theSameElementsAs List("BLOCK")

    new HadoopFileUtil().delete(spark.sparkContext.hadoopConfiguration, outputLocation)
  }

  it should "generate reports for batches and validate all scenarios for NISHTHA reports" in {
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra", new StructType())
      .returning(courseBatchDF)

    val schema = Encoders.product[UserData].schema
    (reporterMock.loadData _)
      .expects(spark, Map("keys.pattern" -> "*","infer.schema" -> "true"),"org.apache.spark.sql.redis", schema)
      .anyNumberOfTimes()
      .returning(userDF)

    CourseMetricsJobV2.loadData(spark, Map("table" -> "user", "keyspace" -> "sunbird"),"org.apache.spark.sql.cassandra", new StructType())


    val convertMethod = udf((value: mutable.WrappedArray[String]) => {
      if(null != value && value.nonEmpty)
        value.toList.map(str => JSONUtils.deserialize(str)(manifest[Map[String, String]])).toArray
      else null
    }, new ArrayType(MapType(StringType, StringType), true))

    val alteredUserCourseDf = userCoursesDF.withColumn("certificates", convertMethod(split(userCoursesDF.col("certificates"), ",").cast("array<string>")) )
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra", new StructType())
      .anyNumberOfTimes()
      .returning(alteredUserCourseDf)

    val outputLocation = "/tmp/course-metrics"
    val outputDir = "course-progress-reports"
    val storageConfig = StorageConfig("local", "", outputLocation)

    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    val strConfig= """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.CourseMetricsJobV2","modelParams":{"reportId": "NISHTHA-reports", "batchFilters":["TPD"],"fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')","sparkCassandraConnectionHost":"'$sunbirdPlatformCassandraHost'","sparkElasticsearchConnectionHost":"'$sunbirdPlatformElasticsearchHost'","sparkRedisConnectionHost":"'$sparkRedisConnectionHost'","sparkUserDbRedisIndex":"12"},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"Course Dashboard Metrics","deviceMapping":false}"""
    val config = JSONUtils.deserialize[JobConfig](strConfig)
    CourseMetricsJobV2.prepareReport(spark, storageConfig, reporterMock.loadData, config, List())

    implicit val sc = spark

    val batchInfo = List(CourseBatch("01303150537737011211","2020-05-29","2030-06-30","b00bc992ef25f1a9a8d63291e20efc8d"), CourseBatch("0130334873750159361","2020-06-11","2030-06-30","013016492159606784174"))
    batchInfo.map(batches => {
      val reportDf = CourseMetricsJobV2.getReportDF(batches,userDF,reporterMock.loadData, config)
      CourseMetricsJobV2.saveReportToBlobStore(batches, reportDf, storageConfig, reportDf.count())
    })

    implicit val batchReportEncoder: Encoder[BatchReportOutput] = Encoders.product[BatchReportOutput]
    val batch1 = "01303150537737011211"
    val batch2 = "0130334873750159361"

    val batchReportsCount = Option(new File(s"$outputLocation/$outputDir").list)
      .map(_.count(_.endsWith(".csv"))).getOrElse(0)

    batchReportsCount should be (2)

    val batch1Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/$outputDir/report-$batch1.csv").as[BatchReportOutput].collectAsList().asScala
    batch1Results.map {res => res.`User ID`}.toList should contain theSameElementsAs List("c7ef3848-bbdb-4219-8344-817d5b8103fa")
    batch1Results.map {res => res.`External ID`}.toList should contain theSameElementsAs List("c98456789-fdcvbn")
    batch1Results.map {res => res.`School UDISE Code`}.toList should contain theSameElementsAs List("20")
    batch1Results.map {res => res.`School Name`}.toList should contain theSameElementsAs List("School-1")
    batch1Results.map {res => res.`Block Name`}.toList should contain theSameElementsAs List("SERA")

    val batch2Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/$outputDir/report-$batch2.csv").as[BatchReportOutput].collectAsList().asScala
    batch2Results.map {res => res.`User ID`}.toList should contain theSameElementsAs List("f3dd58a4-a56f-4c1d-95cf-3231927a28e9")
    batch2Results.map {res => res.`External ID`}.toList should contain theSameElementsAs List("df09619-fdcvbn")
    batch2Results.map {res => res.`School UDISE Code`}.toList should contain theSameElementsAs List("10")
    batch2Results.map {res => res.`School Name`}.toList should contain theSameElementsAs List("School-2")
    batch2Results.map {res => res.`Block Name`}.toList should contain theSameElementsAs List("BLOCK")

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