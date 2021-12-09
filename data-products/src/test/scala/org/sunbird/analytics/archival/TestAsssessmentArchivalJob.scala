package org.sunbird.analytics.archival

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.ekstep.analytics.framework.util.{HadoopFileUtil, JSONUtils}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.exhaust.BaseReportsJob
import org.sunbird.analytics.util.{BaseSpec, EmbeddedCassandra, EmbeddedPostgresql}

class TestAsssessmentArchivalJob extends BaseSpec with MockFactory with BaseReportsJob {

  val outputLocation = "src/test/resources/reports/assessment-archived-data"
  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = getSparkSession();
    super.beforeAll()
    EmbeddedCassandra.loadData("src/test/resources/assessment-archival/data.cql") // Load test data in embedded cassandra server
    EmbeddedPostgresql.start()
    EmbeddedPostgresql.createArchivalRequestTable()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    EmbeddedPostgresql.execute(s"TRUNCATE archival_metadata")
    new HadoopFileUtil().delete(spark.sparkContext.hadoopConfiguration, outputLocation)
  }

  override def afterAll() : Unit = {
    super.afterAll()
    EmbeddedCassandra.close()
    EmbeddedPostgresql.close()
    spark.close()
  }

  "AssessmentArchivalJob" should "archive the batch which is not archived in past" in {
    implicit val fc = new FrameworkContext()
    val batchId = "batch-011"

    val strConfig= """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.$job_name","modelParams":{"mode":"archival","request":{"archivalTable":"assessment_aggregator","query":"{}","batchId":"batch-011","date":"2021-11-01"},"blobConfig":{"store":"azure","blobExt":"csv.gz","reportPath":"assessment-archived-data/","container":"reports"},"sparkCassandraConnectionHost":"{{ core_cassandra_host }}","fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')"},"parallelization":8,"appName":"$job_name"}"""
    implicit val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)

    AssessmentArchivalJob.execute()

    val batch011Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/$batchId/2021*.csv.gz")

    batch011Results.count() should be (5)

    val user1 = batch011Results.filter(col("user_id") === "user-001")
    user1.count() should be (2)

    val user1attempt1 = user1.filter(col("attempt_id") === "attempt-001").first
    user1attempt1.getAs[String]("course_id") should be ("do_1130928636168192001667")
    user1attempt1.getAs[String]("content_id") should be ("do_1128870328040161281204")
    user1attempt1.getAs[String]("last_attempted_on") should be ("2021-12-01T16:51:33.000+05:30")
    user1attempt1.getAs[String]("grand_total") should be ("10")
    user1attempt1.getAs[String]("total_max_score") should be ("10.0")
    user1attempt1.getAs[String]("total_score") should be ("10.0")
    user1attempt1.getAs[String]("question") should be ("[]")
    user1attempt1.getAs[String]("updated_on") should be ("2021-12-01T16:51:33.000+05:30")

    val user1attempt2 = user1.filter(col("attempt_id") === "attempt-002").first

    user1attempt2.getAs[String]("course_id") should be ("do_1130928636168192001667")
    user1attempt2.getAs[String]("content_id") should be ("do_1128870328040161281204")
    user1attempt2.getAs[String]("last_attempted_on") should be (null)
    user1attempt2.getAs[String]("grand_total") should be ("20")
    user1attempt2.getAs[String]("total_max_score") should be ("20.0")
    user1attempt2.getAs[String]("total_score") should be ("20.0")
    val questionsList = JSONUtils.deserialize[List[Map[String, AnyRef]]](user1attempt2.getAs[String]("question"))
    questionsList.size should be (4)

    user1attempt2.getAs[String]("updated_on") should be ("2021-12-09T17:47:34.823+05:30")

    val user2Result = batch011Results.filter(col("user_id") === "user-002")
    user2Result.count() should be (1)

    val user3Result = batch011Results.filter(col("user_id") === "user-003")
    user3Result.count() should be (2)

    val archivalRequests = AssessmentArchivalJob.getRequests(AssessmentArchivalJob.jobId(), Option(batchId))
    archivalRequests.size should be (2)

    archivalRequests.map(ar => ar.request_id).toList should contain allElementsOf List("F08614119F64BC55B14CBE49B10B6730", "949887DE6364A07AE1BB5A04504368F9")
    archivalRequests.map(ar => ar.batch_id).toList.distinct should contain allElementsOf List("batch-011")
    archivalRequests.map(ar => ar.collection_id).toList.distinct should contain allElementsOf List("do_1130928636168192001667")
    archivalRequests.map(ar => ar.archival_status).toList.distinct should contain allElementsOf List("SUCCESS")
    archivalRequests.map(ar => ar.blob_url.get).toList.head.head should include ("src/test/resources/reports/assessment-archived-data/batch-011/2021")
    archivalRequests.map(ar => ar.iteration.get).toList.distinct should contain allElementsOf List(0)
    archivalRequests.map(ar => ar.err_message.get).toList.distinct should contain allElementsOf List("")
  }

  it should "archive the batch which is failed to archive in past" in {
    implicit val fc = new FrameworkContext()
    val batchId = "batch-011"

    EmbeddedPostgresql.execute("INSERT INTO archival_metadata (request_id, batch_id, collection_id , resource_type , job_id , archival_date, completion_date, archival_status, blob_url, iteration,request_data , err_message ) VALUES ('949887DE6364A07AE1BB5A04504368F9', 'batch-011', 'do_1130928636168192001667', 'assessment', 'assessment-archival','2021-12-09 05:58:18.666', null,'FAILED', null, 1,'{\"batchId\": \"batch-011\", \"week\": 48, \"year\": 2021}', NULL);")

    val strConfig= """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.$job_name","modelParams":{"mode":"archival","request":{"archivalTable":"assessment_aggregator","query":"{}","batchId":"batch-011","date":"2021-11-01"},"blobConfig":{"store":"azure","blobExt":"csv.gz","reportPath":"assessment-archived-data/","container":"reports"},"sparkCassandraConnectionHost":"{{ core_cassandra_host }}","fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')"},"parallelization":8,"appName":"$job_name"}"""
    implicit val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)

    AssessmentArchivalJob.execute()

    val batch011Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/$batchId/2021*.csv.gz")

    batch011Results.count() should be (5)

    val user1 = batch011Results.filter(col("user_id") === "user-001")
    user1.count() should be (2)

    val user2Result = batch011Results.filter(col("user_id") === "user-002")
    user2Result.count() should be (1)

    val user3Result = batch011Results.filter(col("user_id") === "user-003")
    user3Result.count() should be (2)

    val archivalRequests = AssessmentArchivalJob.getRequests(AssessmentArchivalJob.jobId(), Option(batchId))
    archivalRequests.size should be (2)

    val failedRequest = AssessmentArchivalJob.getRequest("do_1130928636168192001667", batchId, 2021, 48)

    failedRequest.request_id should be ("949887DE6364A07AE1BB5A04504368F9")
    failedRequest.archival_status should be ("SUCCESS")
    failedRequest.blob_url.get.head should include ("src/test/resources/reports/assessment-archived-data/batch-011/2021")
  }

  it should "skip archival for the batch which is archived in past" in {
    implicit val fc = new FrameworkContext()
    val batchId = "batch-011"

    EmbeddedPostgresql.execute("INSERT INTO archival_metadata (request_id, batch_id, collection_id , resource_type , job_id , archival_date, completion_date, archival_status, blob_url, iteration,request_data , err_message ) VALUES ('949887DE6364A07AE1BB5A04504368F9', 'batch-011', 'do_1130928636168192001667', 'assessment', 'assessment-archival','2021-12-09 05:58:18.666', null,'SUCCESS', '{\"reports/assessment-archival/batch-011/2021-48.csv.gz\"}', 1,'{\"batchId\": \"batch-011\", \"week\": 48, \"year\": 2021}', NULL);")

    val strConfig= """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.$job_name","modelParams":{"mode":"archival","request":{"archivalTable":"assessment_aggregator","query":"{}","batchId":"batch-011","date":"2021-11-01"},"blobConfig":{"store":"azure","blobExt":"csv.gz","reportPath":"assessment-archived-data/","container":"reports"},"sparkCassandraConnectionHost":"{{ core_cassandra_host }}","fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')"},"parallelization":8,"appName":"$job_name"}"""
    implicit val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)

    AssessmentArchivalJob.execute()

    val batch011Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/$batchId/2021*.csv.gz")

    batch011Results.count() should be (3)

    val user1 = batch011Results.filter(col("user_id") === "user-001")
    user1.count() should be (1)
    val user1attempt2 = user1.filter(col("attempt_id") === "attempt-002").first

    user1attempt2.getAs[String]("course_id") should be ("do_1130928636168192001667")
    user1attempt2.getAs[String]("content_id") should be ("do_1128870328040161281204")
    user1attempt2.getAs[String]("last_attempted_on") should be (null)
    user1attempt2.getAs[String]("grand_total") should be ("20")
    user1attempt2.getAs[String]("total_max_score") should be ("20.0")
    user1attempt2.getAs[String]("total_score") should be ("20.0")

    val user2Result = batch011Results.filter(col("user_id") === "user-002")
    user2Result.count() should be (1)

    val user3Result = batch011Results.filter(col("user_id") === "user-003")
    user3Result.count() should be (1)

    val archivalRequests = AssessmentArchivalJob.getRequests(AssessmentArchivalJob.jobId(), Option(batchId))
    archivalRequests.size should be (2)
  }

}
