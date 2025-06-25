package org.sunbird.analytics.job.report

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{HadoopFileUtil, JSONUtils}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.util.{EmbeddedPostgresql, UserData}

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

    EmbeddedPostgresql.start()
    EmbeddedPostgresql.createUserSummaryReport()

  }

  override def afterAll(): Unit = {
    super.afterAll()
    val objectKey = AppConf.getConfig("course.metrics.cloud.objectKey")
    new HadoopFileUtil().delete(spark.sparkContext.hadoopConfiguration, objectKey + "collection-summary-reports-v2/")
    EmbeddedPostgresql.close()
  }

  val convertMethod = udf((value: mutable.WrappedArray[String]) => {
    if (null != value && value.nonEmpty)
      value.toList.map(str => JSONUtils.deserialize(str)(manifest[Map[String, String]])).toArray
    else null
  })

  it should "generate the report for all the batches" in {
    initializeDefaultMockData()
    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.CollectionSummaryJobV2","modelParams":{"searchFilter":{"request":{"filters":{"status":["Live"],"contentType":"Course"},"fields":["identifier","name","organisation","channel","status","keywords","createdFor","medium", "subject"],"limit":10000}},"store":"azure","sparkElasticsearchConnectionHost":"{{ sunbird_es_host }}","sparkRedisConnectionHost":"{{ metadata2_redis_host }}","sparkUserDbRedisIndex":"12","sparkCassandraConnectionHost":"{{ core_cassandra_host }}","fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')","specPath":"src/test/resources/ingestion-spec/summary-ingestion-spec.json"},"parallelization":8,"appName":"Collection Summary Report"}""".stripMargin
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    val reportData = CollectionSummaryJobV2.prepareReport(spark, reporterMock.fetchData)

    // Read and print all records from user_summary_report table in embedded Postgres
    import java.sql.DriverManager
    val conn = DriverManager.getConnection("jdbc:postgresql://localhost:65124/postgres", "postgres", "postgres")
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT * FROM user_summary_report")
    println("\n--- user_summary_report table contents ---")
    val meta = rs.getMetaData
    val colCount = meta.getColumnCount
    // Print column headers
    val headers = (1 to colCount).map(meta.getColumnName).mkString(", ")
    println(headers)
    // Print each row
    while (rs.next()) {
      val row = (1 to colCount).map(i => Option(rs.getObject(i)).getOrElse("")).mkString(", ")
      println(row)
    }
    println("--- end of user_summary_report table ---\n")
    rs.close()
    stmt.close()
    conn.close()
  }


  def initializeDefaultMockData() {
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