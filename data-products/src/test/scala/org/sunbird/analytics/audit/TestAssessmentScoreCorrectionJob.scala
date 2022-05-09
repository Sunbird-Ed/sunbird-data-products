package org.sunbird.analytics.audit

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.util.{HadoopFileUtil, JSONUtils}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.job.report.BaseReportSpec
import org.sunbird.analytics.util.EmbeddedCassandra

import scala.collection.immutable.List

class TestAssessmentScoreCorrectionJob extends BaseReportSpec with MockFactory {
  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession();
  }

  override def beforeEach(): Unit = {
    EmbeddedCassandra.loadData("src/test/resources/assessment-score-correction/assessment.cql") // Load test data in embedded cassandra server
  }

  override def afterEach(): Unit = {
    EmbeddedCassandra.close()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    EmbeddedCassandra.close()
    new HadoopFileUtil().delete(spark.sparkContext.hadoopConfiguration, "src/test/resources/score-metrics-migration-job/")
    spark.close()
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


    // Validate Deleted Records Validation
    val batch_id = "batch-00001"
    val course_id = "do_11307972307046400011917"
    val deleted_records = spark.read.format("com.databricks.spark.csv").option("header", "true")
      .load(s"src/test/resources/score-metrics-migration-job/assessment-invalid-attempts-records-$batch_id-$course_id-*.csv/*.csv").cache()
    deleted_records.count() should be(2)

    val contentIds: List[String] = deleted_records.select("content_id").distinct().collect().map(_ (0)).toList.asInstanceOf[List[String]]
    contentIds.distinct.size should be(1)
    contentIds.head should be("do_11307972307046400011917")

    val attempt_ids: List[String] = deleted_records.select("attempt_id").distinct().collect().map(_ (0)).toList.asInstanceOf[List[String]]
    attempt_ids.distinct.size should be(2)
    attempt_ids(0) should be("attempat-001")
    attempt_ids(1) should be("attempat-002")

    val user_id: List[String] = deleted_records.select("user_id").distinct().collect().map(_ (0)).toList.asInstanceOf[List[String]]
    user_id.distinct.size should be(1)
    user_id.head should be("user-001")

    // Validate the instruction Events

    val instructionEvents = spark.read.json(s"src/test/resources/score-metrics-migration-job/instruction-events-$batch_id-*.json").cache()

    val contentId: String = instructionEvents.select("contentId").distinct().collect().map(_ (0)).toList.head.asInstanceOf[String]
    val courseId: String = instructionEvents.select("courseId").distinct().collect().map(_ (0)).toList.head.asInstanceOf[String]
    val userId: String = instructionEvents.select("userId").distinct().collect().map(_ (0)).toList.head.asInstanceOf[String]

    contentId should be("do_11307972307046400011917")
    courseId should be("do_1130928636168192001667")
    userId should be("user-001")

    // Validate the cert data

    val cert_data = spark.read.format("com.databricks.spark.csv").option("header", "true")
      .load(s"src/test/resources/score-metrics-migration-job/revoked-cert-data-$batch_id-$course_id-*.csv/*.csv").cache()

    cert_data.show(false)
    cert_data.count() should be(1)
    cert_data.select("courseid").distinct().collect().map(_ (0)).toList.head.asInstanceOf[String] should be("do_1130928636168192001667")
    cert_data.select("batchid").distinct().collect().map(_ (0)).toList.head.asInstanceOf[String] should be("batch-00001")
    cert_data.select("userid").distinct().collect().map(_ (0)).toList.head.asInstanceOf[String] should be("user-001")
    cert_data.select("certificate_id").distinct().collect().map(_ (0)).toList.head.asInstanceOf[String] should be("e08017de-3cb7-47d2-a375-6dd3f8575806")


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
