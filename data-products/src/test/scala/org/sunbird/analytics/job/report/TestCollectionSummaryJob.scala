package org.sunbird.analytics.job.report

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig, StorageConfig}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.util.UserData

class TestCollectionSummaryJob extends BaseReportSpec with MockFactory {


  var spark: SparkSession = _

  var courseBatchDF: DataFrame = _
  var userEnrolments: DataFrame = _
  var userDF: DataFrame = _
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
      .load("src/test/resources/course-metrics-updaterv2/course_batch_data.csv")
      .cache()

    userEnrolments = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/course-metrics-updaterv2/user_courses_data.csv")
      .cache()

    userDF = spark.read.json("src/test/resources/course-metrics-updaterv2/user_data.json")
      .cache()

  }

  it should "generate the report for all the batches" in {

    (reporterMock.fetchData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace), "org.apache.spark.sql.cassandra", new StructType())
      .returning(courseBatchDF)

    (reporterMock.fetchData _)
      .expects(spark, Map("table" -> "user_enrolments", "keyspace" -> sunbirdCoursesKeyspace, "cluster" -> "LMSCluster"), "org.apache.spark.sql.cassandra", new StructType())
      .returning(userEnrolments)
      .anyNumberOfTimes()

    val schema = Encoders.product[UserData].schema
    (reporterMock.fetchData _)
      .expects(spark, Map("table" -> "user", "infer.schema" -> "true", "key.column" -> "userid"), "org.apache.spark.sql.redis", schema)
      .anyNumberOfTimes()
      .returning(userDF)


    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    val strConfig = """{"search": {"type": "none"},"model": "org.sunbird.analytics.job.report.CourseMetricsJob","modelParams": {"batchFilters": ["TPD"],"fromDate": "$(date --date yesterday '+%Y-%m-%d')","toDate": "$(date --date yesterday '+%Y-%m-%d')","sparkCassandraConnectionHost": "127.0.0.0","sparkElasticsearchConnectionHost": "'$sunbirdPlatformElasticsearchHost'","sparkRedisConnectionHost": "'$sparkRedisConnectionHost'","sparkUserDbRedisIndex": "4","contentFilters": {"request": {"filters": {"framework": "TPD"},"sort_by": {"createdOn": "desc"},"limit": 10000,"fields": ["framework", "identifier", "name", "channel"]}},"reportPath": "course-reports/"},"output": [{"to": "console","params": {"printEvent": false}}],"parallelization": 8,"appName": "Course Dashboard Metrics","deviceMapping": false}""".stripMargin
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    val storageConfig = StorageConfig("local", "", "/tmp/course-metrics")
    CollectionSummaryJob.prepareReport(spark, reporterMock.fetchData, jobConfig, List())
  }


}