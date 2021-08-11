package org.sunbird.analytics.job.report


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{HadoopFileUtil, JSONUtils}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.util.EmbeddedCassandra


class TestAssessmentArchivalJob extends BaseReportSpec with MockFactory {

  var spark: SparkSession = _

  var assessmentAggDF: DataFrame = _
  var reporterMock: BaseReportsJob = mock[BaseReportsJob]
  val sunbirdCoursesKeyspace = "sunbird_courses"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession();
    EmbeddedCassandra.loadData("src/test/resources/assessment-archival/assessment_agg.cql") // Load test data in embedded cassandra server
  }

  override def afterAll(): Unit = {
    super.afterAll()
    val objectKey = AppConf.getConfig("course.metrics.cloud.objectKey")
    new HadoopFileUtil().delete(spark.sparkContext.hadoopConfiguration, objectKey + "assessment-archival")
  }


  it should "Should able to archive the batch data" in {
    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.AssessmentArchivalJob","modelParams":{"truncateData":false,"store":"local","sparkCassandraConnectionHost":"{{ core_cassandra_host }}","fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')"},"parallelization":8,"appName":"Assessment Archival Job"}""".stripMargin
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    val reportData = AssessmentArchivalJob.archiveData(spark, jobConfig)

    val batch_1 = reportData.filter(x => x.getOrElse("batch_id", "").asInstanceOf[String] === "batch-001")
    batch_1.foreach(res => res("year") === "2021")
    batch_1.foreach(res => res("total_records") === "5")
    batch_1.foreach(res => res("week_of_year") === "32")

    val batch_2 = reportData.filter(x => x.getOrElse("batch_id", "").asInstanceOf[String] === "batch-004")
    batch_2.foreach(res => res("year") === "2021")
    batch_2.foreach(res => res("total_records") === "1")
    batch_2.foreach(res => res("week_of_year") === "32")

  }

}