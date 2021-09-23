package org.sunbird.analytics.job.report


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{HadoopFileUtil, JSONUtils}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.util.EmbeddedCassandra


class TestAssessmentArchivalJob extends BaseReportSpec with MockFactory {

  implicit var spark: SparkSession = _

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
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.AssessmentArchivalJob","modelParams":{"deleteArchivedBatch":false,"store":"local","sparkCassandraConnectionHost":"{{ core_cassandra_host }}","fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')"},"parallelization":8,"appName":"Assessment Archival Job"}""".stripMargin
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    val reportData = AssessmentArchivalJob.archiveData(date = null, None, archiveForLastWeek = true)

    val batch_1 = reportData.filter(x => x.batchId.getOrElse("") === "batch-001")
    batch_1.foreach(res => res.period.year === "2021")
    batch_1.foreach(res => res.totalArchivedRecords === "5")
    batch_1.foreach(res => res.period.weekOfYear === "32")

    val batch_2 = reportData.filter(x => x.batchId.getOrElse("") === "batch-004")
    batch_2.foreach(res => res.period.year === "2021")
    batch_2.foreach(res => res.totalArchivedRecords === "1")
    batch_2.foreach(res => res.period.weekOfYear === "32")
  }

  it should "Should able to fetch the archived records from the azure and delete the records" in {
    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.AssessmentArchivalJob","modelParams":{"archivalFetcherConfig":{"store":"local","format":"csv.gz","reportPath":"src/test/resources/assessment-archival/archival-data/","container":""},"deleteArchivedBatch":true,"sparkCassandraConnectionHost":"{{ core_cassandra_host }}","fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')"},"parallelization":8,"appName":"Assessment Archival Job"}""".stripMargin
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    val reportData = AssessmentArchivalJob.removeRecords(date = "2021-08-18", None, archiveForLastWeek = false)
    reportData.head.totalDeletedRecords.get should be(6)
  }



}