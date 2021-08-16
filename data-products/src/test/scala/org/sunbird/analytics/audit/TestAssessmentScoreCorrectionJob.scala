package org.sunbird.analytics.audit

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.job.report.{BaseReportSpec, BaseReportsJob}
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

  it should "Should able correct the records" in {
    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    implicit val sc: SparkContext = spark.sparkContext
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.AssessmentScoreCorrectionJob","modelParams":{"isDryRunMode":false,"csvPath":"","store":"local","sparkCassandraConnectionHost":"{{ core_cassandra_host }}","fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')"},"parallelization":8,"appName":"Assessment Score Correction"}""".stripMargin
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    val reportData = AssessmentScoreCorrectionJob.processBatches()
    //val values =  AssessmentScoreCorrectionJob.getTotalQuestions("do_313026415363981312122", "https://diksha.gov.in/api/content/v1/read/")

    //println("total_score" + Await.result[Int](values, 60.seconds))

  }


}
