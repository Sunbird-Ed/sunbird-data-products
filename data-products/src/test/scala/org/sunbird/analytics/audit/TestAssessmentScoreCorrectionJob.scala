package org.sunbird.analytics.audit

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.exhaust.BaseReportsJob
import org.sunbird.analytics.job.report.BaseReportSpec
import org.sunbird.analytics.util.EmbeddedCassandra

class TestAssessmentScoreCorrectionJob extends BaseReportSpec with MockFactory {
  implicit var spark: SparkSession = _

  var assessmentAggDF: DataFrame = _
  var reporterMock: BaseReportsJob = mock[BaseReportsJob]
  val sunbirdCoursesKeyspace = "sunbird_courses"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession();
    EmbeddedCassandra.loadData("src/test/resources/assessment-score-correction/assessment.cql") // Load test data in embedded cassandra server
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  it should "Should able correct assessment raw data records when dryRunMode is true" in {
    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    implicit val sc: SparkContext = spark.sparkContext
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.AssessmentScoreCorrectionJob","modelParams":{"contentReadAPI":"https://dev.sunbirded.org/api/content/v1/read/","assessment.score.correction.batches":["batch-00001"],"isDryRunMode":true,"csvPath":"src/test/resources/score-metrics-migration-job/","store":"local","sparkCassandraConnectionHost":"{{ core_cassandra_host }}","fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')"},"parallelization":8,"appName":"Assessment Score Correction"}""".stripMargin
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    val reportData = AssessmentScoreCorrectionJob.processBatches()
    reportData.foreach(report => {
      report.foreach(data => {
        data.batchId should be("batch-00001")
        data.invalidRecords should be(2)
        data.contentId should be("do_11307972307046400011917")
        data.contentTotalQuestions should be(4)
        data.totalAffectedUsers should be(1)
      })
    })
  }

  it should "Should able correct assessment raw data records when dryRunMode is false" in {
    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    implicit val sc: SparkContext = spark.sparkContext
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.AssessmentScoreCorrectionJob","modelParams":{"correctRawAssessment":true,"contentReadAPI":"https://dev.sunbirded.org/api/content/v1/read/","assessment.score.correction.batches":["batch-00001"],"isDryRunMode":false,"csvPath":"src/test/resources/score-metrics-migration-job/","store":"local","sparkCassandraConnectionHost":"{{ core_cassandra_host }}","fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')"},"parallelization":8,"appName":"Assessment Score Correction"}""".stripMargin
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    val reportData = AssessmentScoreCorrectionJob.processBatches()
    reportData.foreach(report => {
      report.foreach(data => {
        data.batchId should be("batch-00001")
        data.invalidRecords should be(2)
        data.contentId should be("do_11307972307046400011917")
        data.contentTotalQuestions should be(4)
        data.totalAffectedUsers should be(1)
      })
    })
  }
}
