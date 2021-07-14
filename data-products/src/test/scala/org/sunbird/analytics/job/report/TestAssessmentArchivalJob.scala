package org.sunbird.analytics.job.report


import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{HadoopFileUtil, JSONUtils}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.scalamock.scalatest.MockFactory

import scala.collection.mutable


class TestAssessmentArchivalJob extends BaseReportSpec with MockFactory {

  var spark: SparkSession = _

  var assessmentAggDF: DataFrame = _
  var reporterMock: BaseReportsJob = mock[BaseReportsJob]
  val sunbirdCoursesKeyspace = "sunbird_courses"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession();
    assessmentAggDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/assessment-archival/assessment_aggregator.csv")
      .cache()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    val objectKey = AppConf.getConfig("course.metrics.cloud.objectKey")
    new HadoopFileUtil().delete(spark.sparkContext.hadoopConfiguration, objectKey + "collection-summary-reports-v2/")
  }

  val convertMethod = udf((value: mutable.WrappedArray[String]) => {
    if (null != value && value.nonEmpty)
      value.toList.map(str => JSONUtils.deserialize(str)(manifest[Map[String, String]])).toArray
    else null
  }, new ArrayType(MapType(StringType, StringType), true))

  it should "Should able to archive the batch data" in {
    initializeDefaultMockData()
    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.AssessmentArchivalJob","modelParams":{"truncateData":false,"store":"local","sparkCassandraConnectionHost":"{{ core_cassandra_host }}","fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')"},"parallelization":8,"appName":"Assessment Archival Job"}""".stripMargin
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    val reportData = AssessmentArchivalJob.archiveData(spark, reporterMock.fetchData, "/Users/manjunathdavanam/Documents/Projects.nosync/Sunbird/partition-data", jobConfig)

    val batch_1 = reportData.filter(x => x.getOrElse("batch_id", "").asInstanceOf[String] === "1010")
    batch_1.foreach(res => res("year") === "2019")
    batch_1.foreach(res => res("total_records") === "2")
    batch_1.foreach(res => res("week_of_year") === "36")


    val batch_2 = reportData.filter(x => x.getOrElse("batch_id", "").asInstanceOf[String] === "1001")
    batch_2.foreach(res => res("year") === "2019")
    batch_2.foreach(res => res("total_records") === "3")
    batch_2.foreach(res => res("week_of_year") === "36")


    val batch_3 = reportData.filter(x => x.getOrElse("batch_id", "").asInstanceOf[String] === "1005")
    batch_3.foreach(res => res("year") === "2019")
    batch_3.foreach(res => res("total_records") === "1")
    batch_3.foreach(res => res("week_of_year") === "36")


    val batch_4 = reportData.filter(x => x.getOrElse("batch_id", "").asInstanceOf[String] === "1006")
    batch_4.foreach(res => res("year") === "2019")
    batch_4.foreach(res => res("total_records") === "2")
    batch_4.foreach(res => res("week_of_year") === "36")

  }

  def initializeDefaultMockData() {
    (reporterMock.fetchData _)
      .expects(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace, "cluster" -> "LMSCluster"), "org.apache.spark.sql.cassandra", new StructType())
      .returning(assessmentAggDF)
  }
}