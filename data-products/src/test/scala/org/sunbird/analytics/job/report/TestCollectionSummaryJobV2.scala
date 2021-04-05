package org.sunbird.analytics.job.report

import java.io.File

import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.util.UserData

import scala.collection.mutable


class TestCollectionSummaryJobV2 extends BaseReportSpec with MockFactory {


  var spark: SparkSession = _

  var courseBatchDF: DataFrame = _
  var userEnrolments: DataFrame = _
  var userDF: DataFrame = _
  var organisationDF: DataFrame = _
  var reporterMock: BaseReportsJob = mock[BaseReportsJob]
  val sunbirdCoursesKeyspace = "sunbird_courses"
  val sunbirdKeyspace = "sunbird"
  val esIndexName = "composite"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession();
    courseBatchDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/collection-summary/course_batch_data.csv")
      .cache()

    userEnrolments = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/collection-summary/user_courses_data.csv")
      .cache()

    userDF = spark.read.json("src/test/resources/collection-summary/user_data.json")
      .cache()

  }

  val convertMethod = udf((value: mutable.WrappedArray[String]) => {
    if (null != value && value.nonEmpty)
      value.toList.map(str => JSONUtils.deserialize(str)(manifest[Map[String, String]])).toArray
    else null
  }, new ArrayType(MapType(StringType, StringType), true))

  it should "generate the report for all the batches" in {
    initializeDefaultMockData()
    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.CollectionSummaryJobV2","modelParams":{"searchFilter":{"request":{"filters":{"status":["Live"],"contentType":"Course"},"fields":["identifier","name","organisation","channel","status","keywords"],"limit":10000}},"store":"azure","sparkElasticsearchConnectionHost":"{{ sunbird_es_host }}","sparkRedisConnectionHost":"{{ metadata2_redis_host }}","sparkUserDbRedisIndex":"12","sparkCassandraConnectionHost":"{{ core_cassandra_host }}","fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')","specPath":"src/test/resources/ingestion-spec/summary-ingestion-spec.json"},"parallelization":8,"appName":"Collection Summary Report"}""".stripMargin
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    val reportData = CollectionSummaryJobV2.prepareReport(spark, reporterMock.fetchData)
    reportData.count() should be(3)
    val batch1 = reportData.filter(col("batchid") === "batch-0130293763489873929" && col("courseid") === "do_1130293726460805121168")
    batch1.select("state").collect().map(_ (0)).toList.contains("KA") should be(true)
    batch1.select("district").collect().map(_ (0)).toList.contains("BG") should be(true)
    batch1.select("enrolleduserscount").collect().map(_ (0)).toList.contains(2) should be(true)
    batch1.select("completionuserscount").collect().map(_ (0)).toList.contains(0) should be(true)
    batch1.select("hascertified").collect().map(_ (0)).toList.contains("N") should be(true)
    batch1.select("certificateissuedcount").collect().map(_ (0)).toList.contains(0) should be(true)
    batch1.select("collectionname").collect().map(_ (0)).toList.contains("Test") should be(true)
    batch1.select("contentorg").collect().map(_ (0)).toList.size shouldNot be(0)
    batch1.select("channel").collect().map(_ (0)).toList.contains("013016492159606784174") should be(true)
    batch1.select("enddate").collect().map(_ (0)).toList.contains("2030-06-30") should be(true)
    batch1.select("startdate").collect().map(_ (0)).toList.contains("2020-05-26") should be(true)

    val batch2 = reportData.filter(col("batchid") === "batch-0130320389509939204" && col("courseid") === "do_112636984058314752121")
    batch2.select("state").collect().map(_ (0)).toList.contains("GPPS") should be(true)
    batch2.select("district").collect().map(_ (0)).toList.contains("MPPS") should be(true)
    batch2.select("enrolleduserscount").collect().map(_ (0)).toList.contains(2) should be(true)
    batch2.select("completionuserscount").collect().map(_ (0)).toList.contains(2) should be(true)
    batch2.select("hascertified").collect().map(_ (0)).toList.contains("N") should be(true)
    batch2.select("certificateissuedcount").collect().map(_ (0)).toList.contains(0) should be(true)
    batch2.select("collectionname").collect().map(_ (0)).toList.contains("SB-6729 course notification test") should be(true)
    batch1.select("contentorg").collect().map(_ (0)).toList.size shouldNot be(0)
    batch2.select("channel").collect().map(_ (0)).toList.contains("b00bc992ef25f1a9a8d63291e20efc8d") should be(true)
    batch2.select("enddate").collect().map(_ (0)).toList.contains("2030-06-30") should be(true)
    batch2.select("startdate").collect().map(_ (0)).toList.contains("2020-05-30") should be(true)

    val batch3 = reportData.filter(col("batchid") === "batch-01303150537737011211" && col("courseid") === "do_1130314965721088001129")
    batch3.select("state").collect().map(_ (0)).toList.contains("KA") should be(true)
    batch3.select("district").collect().map(_ (0)).toList.contains("BG") should be(true)
    batch3.select("enrolleduserscount").collect().map(_ (0)).toList.contains(2) should be(true)
    batch3.select("completionuserscount").collect().map(_ (0)).toList.contains(0) should be(true)
    batch3.select("hascertified").collect().map(_ (0)).toList.contains("N") should be(true)
    batch3.select("certificateissuedcount").collect().map(_ (0)).toList.contains(0) should be(true)
    batch1.select("contentorg").collect().map(_ (0)).toList.size shouldNot be(0)
    batch3.select("channel").collect().map(_ (0)).toList.contains("b00bc992ef25f1a9a8d63291e20efc8d") should be(true)
    batch3.select("enddate").collect().map(_ (0)).toList.contains("2030-06-30") should be(true)
    batch3.select("startdate").collect().map(_ (0)).toList.contains("2020-05-29") should be(true)
    CollectionSummaryJobV2.saveToBlob(reportData, jobConfig)
  }

  it should "generate report when searchfilter config is not defined in the jobconfig" in {
    initializeDefaultMockData()
    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.CollectionSummaryJobV2","modelParams":{"store":"azure","sparkElasticsearchConnectionHost":"{{ sunbird_es_host }}","sparkRedisConnectionHost":"{{ metadata2_redis_host }}","sparkUserDbRedisIndex":"12","sparkCassandraConnectionHost":"{{ core_cassandra_host }}","fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')","specPath":"src/test/resources/ingestion-spec/summary-ingestion-spec.json"},"parallelization":8,"appName":"Collection Summary Report"}""".stripMargin
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    CollectionSummaryJobV2.prepareReport(spark, reporterMock.fetchData).count() should be(3)
  }

  it should "generate report when only batchStartDate defined in the jobconfig" in {
    initializeDefaultMockData()
    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.CollectionSummaryJobV2","modelParams":{"store":"azure","batchStartDate":"2020-05-29","sparkElasticsearchConnectionHost":"{{ sunbird_es_host }}","sparkRedisConnectionHost":"{{ metadata2_redis_host }}","sparkUserDbRedisIndex":"12","sparkCassandraConnectionHost":"{{ core_cassandra_host }}","fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')","specPath":"src/test/resources/ingestion-spec/summary-ingestion-spec.json"},"parallelization":8,"appName":"Collection Summary Report"}""".stripMargin
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    val report = CollectionSummaryJobV2.prepareReport(spark, reporterMock.fetchData)
    report.count() should be(2)
  }

  it should "generate report when only generateForAllBatches defined in the jobconfig" in {
    initializeDefaultMockData()
    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.CollectionSummaryJobV2","modelParams":{"store":"azure","generateForAllBatches":true,"sparkElasticsearchConnectionHost":"{{ sunbird_es_host }}","sparkRedisConnectionHost":"{{ metadata2_redis_host }}","sparkUserDbRedisIndex":"12","sparkCassandraConnectionHost":"{{ core_cassandra_host }}","fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')","specPath":"src/test/resources/ingestion-spec/summary-ingestion-spec.json"},"parallelization":8,"appName":"Collection Summary Report"}""".stripMargin
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    CollectionSummaryJobV2.prepareReport(spark, reporterMock.fetchData).count() should be(3)
  }

  it should "generate report when  generateForAllBatches,searchFilter & startDate configs are not defined in the jobconfig" in {
    initializeDefaultMockData()
    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.CollectionSummaryJobV2","modelParams":{"store":"azure","sparkElasticsearchConnectionHost":"{{ sunbird_es_host }}","sparkRedisConnectionHost":"{{ metadata2_redis_host }}","sparkUserDbRedisIndex":"12","sparkCassandraConnectionHost":"{{ core_cassandra_host }}","fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')","specPath":"src/test/resources/ingestion-spec/summary-ingestion-spec.json"},"parallelization":8,"appName":"Collection Summary Report"}""".stripMargin
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    CollectionSummaryJobV2.prepareReport(spark, reporterMock.fetchData).count() should be(3)
  }

  it should "submit the ingestion file" in {
    val ingestionServerMockURL = "https://httpbin.org/post"
    val resourceName = "ingestion-spec/summary-ingestion-spec.json"
    val classLoader = getClass.getClassLoader
    val file = new File(classLoader.getResource(resourceName).getFile)
    val absolutePath = file.getAbsolutePath
    CollectionSummaryJobV2.submitIngestionTask(ingestionServerMockURL, absolutePath)
  }
  def initializeDefaultMockData() {
    (reporterMock.fetchData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace, "cluster" -> "LMSCluster"), "org.apache.spark.sql.cassandra", new StructType())
      .returning(courseBatchDF.withColumn("cert_templates", lit(null).cast(MapType(StringType, MapType(StringType, StringType)))))

    (reporterMock.fetchData _)
      .expects(spark, Map("table" -> "user_enrolments", "keyspace" -> sunbirdCoursesKeyspace, "cluster" -> "ReportCluster"), "org.apache.spark.sql.cassandra", new StructType())
      .returning(userEnrolments.withColumn("certificates", convertMethod(split(userEnrolments.col("certificates"), ",").cast("array<string>")))
        .withColumn("issued_certificates", convertMethod(split(userEnrolments.col("issued_certificates"), ",").cast("array<string>")))
      )
      .anyNumberOfTimes()

    val schema = Encoders.product[UserData].schema
    (reporterMock.fetchData _)
      .expects(spark, Map("table" -> "user", "infer.schema" -> "true", "key.column" -> "userid"), "org.apache.spark.sql.redis", schema)
      .anyNumberOfTimes()
      .returning(userDF)
  }
}