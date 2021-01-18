package org.sunbird.analytics.audit

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{lit, split, udf}
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.ekstep.analytics.framework.util.JSONUtils
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.job.report.CollectionSummaryJob.saveToBlob
import org.sunbird.analytics.job.report.{BaseReportSpec, BaseReportsJob, CollectionSummaryJob}
import org.sunbird.analytics.util.UserData

import scala.collection.mutable

object TestCourseBatchStatusUpdater extends BaseReportSpec with MockFactory {

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
      .load("src/test/resources/collection-summary/course_batch_data.csv")
      .cache()

  }


  it should "Should update the status of the course/batch" in intercept[Exception] {

    (reporterMock.fetchData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace, "cluster" -> "LMSCluster"), "org.apache.spark.sql.cassandra", new StructType())
      .returning(courseBatchDF.withColumn("cert_templates", lit(null).cast(MapType(StringType, MapType(StringType, StringType)))))

    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.audit.CourseBatchStatusUpdaterJob","modelParams":{"store":"azure","sparkElasticsearchConnectionHost":"http://localhost:9200","sparkCassandraConnectionHost":"localhost","kpLearningBasePath":"http://localhost:8080/learning-service","fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')"},"parallelization":8,"appName":"Course Batch Status Updater Job"}""".stripMargin
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val sc: SparkContext = spark.sparkContext
    CourseBatchStatusUpdaterJob.execute(reporterMock.fetchData)
  }

}
