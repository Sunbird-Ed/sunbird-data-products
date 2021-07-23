package org.sunbird.analytics.audit

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, lit, split, to_date, udf}
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.job.report.{BaseReportSpec, BaseReportsJob}

import java.text.SimpleDateFormat
import java.util.TimeZone
import scala.collection.mutable

class TestCourseBatchStatusUpdater extends BaseReportSpec with MockFactory {
  implicit var spark: SparkSession = _

  var courseBatchDF: DataFrame = _
  var organisationDF: DataFrame = _
  var reporterMock: BaseReportsJob = mock[BaseReportsJob]
  val sunbirdCoursesKeyspace = "sunbird_courses"


  val convertMethod = udf((value: mutable.WrappedArray[String]) => {
    if (null != value && value.nonEmpty)
      value.toList.map(str => JSONUtils.deserialize(str)(manifest[Map[String, String]])).toArray
    else null
  }, new ArrayType(MapType(StringType, StringType), true))


  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession();
    courseBatchDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/course-batch-status-updater/course_batch_status_updater_temp.csv")
      .cache()

  }

  it should "Should update the status of the course/batch" in {
    (reporterMock.fetchData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace, "cluster" -> "LMSCluster"), "org.apache.spark.sql.cassandra", new StructType())
      .returning(courseBatchDF.withColumn("cert_templates", lit(null).cast(MapType(StringType, MapType(StringType, StringType)))))
    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.audit.CourseBatchStatusUpdaterJob","modelParams":{"store":"azure","sparkElasticsearchConnectionHost":"http://localhost:9200","sparkCassandraConnectionHost":"localhost","kpLearningBasePath":"http://localhost:8080/learning-service","fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')"},"parallelization":8,"appName":"Course Batch Status Updater Job"}""".stripMargin
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val sc: SparkContext = spark.sparkContext
    val res = CourseBatchStatusUpdaterJob.execute(reporterMock.fetchData)
    res.inProgress should be(1)
    res.completed should be(2)
    res.unStarted should be(0)
  }

  it should "Get the valid getEnrolmentEndDate" in {
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
    dateFormatter.setTimeZone(TimeZone.getTimeZone("IST"))
    CourseBatchStatusUpdaterJob.getEnrolmentEndDate(null, "2030-06-30", dateFormatter) should be("2030-06-29")
    CourseBatchStatusUpdaterJob.getEnrolmentEndDate("2030-06-30", null, dateFormatter) should be("2030-06-30")
    CourseBatchStatusUpdaterJob.getEnrolmentEndDate(null, null, dateFormatter) should be(null)
  }

  it should "not update the status of course/batch as finalDF is empty" in {
    val courseBatchDF2 = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/course-batch-status-updater/course_batch_status_updater_temp2.csv")
      .cache()

    (reporterMock.fetchData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace, "cluster" -> "LMSCluster"), "org.apache.spark.sql.cassandra", new StructType())
      .returning(courseBatchDF2.withColumn("cert_templates", lit(null).cast(MapType(StringType, MapType(StringType, StringType)))))
    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.audit.CourseBatchStatusUpdaterJob","modelParams":{"store":"azure","sparkElasticsearchConnectionHost":"http://localhost:9200","sparkCassandraConnectionHost":"localhost","kpLearningBasePath":"http://localhost:8080/learning-service","fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')"},"parallelization":8,"appName":"Course Batch Status Updater Job"}""".stripMargin
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val sc: SparkContext = spark.sparkContext
    val res = CourseBatchStatusUpdaterJob.execute(reporterMock.fetchData)

    res.inProgress should be(0)
    res.completed should be(0)
    res.unStarted should be(0)
  }

  it should "execute the job with default config" in {
    (reporterMock.fetchData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace, "cluster" -> "LMSCluster"), "org.apache.spark.sql.cassandra", new StructType())
      .returning(courseBatchDF.withColumn("cert_templates", lit(null).cast(MapType(StringType, MapType(StringType, StringType)))))
    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.audit.CourseBatchStatusUpdaterJob","parallelization":8,"appName":"Course Batch Status Updater Job"}""".stripMargin
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val sc: SparkContext = spark.sparkContext
    val res = CourseBatchStatusUpdaterJob.execute(reporterMock.fetchData)
    res.inProgress should be(1)
    res.completed should be(2)
    res.unStarted should be(0)
  }

  /*
   * Testcase for getting the latest value from migrated date fields
   * enrolleddate: (Old Field)
   *   null
   *   2020-02-25 13:18:36:051+0000
   *   2018-10-29 17:47:52:601+0000
   * enrolled_date: (New Field)
   *   2020-02-25 13:18:36:051+0000
   *   null
   *   2018-10-31 17:47:52:601+0000
   * expected result enrolleddate:
   *   2020-02-25 13:18:36:051+0000
   *   2020-02-25 13:18:36:051+0000
   *   2018-10-31 17:47:52:601+0000
   */
  it should "get the latest value for date" in {
    val courseBatchDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/course-batch-status-updater/course_batch_status_updater_temp.csv")
      .cache()
    (reporterMock.fetchData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace, "cluster" -> "LMSCluster"), "org.apache.spark.sql.cassandra", new StructType())
      .returning(courseBatchDF.withColumn("cert_templates", lit(null).cast(MapType(StringType, MapType(StringType, StringType)))))
    val resultDf = CourseBatchStatusUpdaterJob.getCollectionBatchDF(reporterMock.fetchData)
    val resultDateValue =  resultDf.select("startdate").collect().map(_ (0)).toList
    resultDateValue(0) should be("2021-04-28")
    resultDateValue(1) should be("2020-05-26")
    resultDateValue(2) should be("2020-05-29")
    resultDateValue(3) should be("2020-05-25")
  }
}
