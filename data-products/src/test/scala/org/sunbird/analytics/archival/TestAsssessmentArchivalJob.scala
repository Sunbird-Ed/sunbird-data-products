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
    EmbeddedPostgresql.start()
    EmbeddedPostgresql.createArchivalRequestTable()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    EmbeddedCassandra.close()
    EmbeddedPostgresql.execute(s"TRUNCATE archival_metadata")
  }

  override def beforeEach(): Unit = {
    EmbeddedCassandra.loadData("src/test/resources/assessment-archival/data.cql") // Load test data in embedded cassandra server
    new HadoopFileUtil().delete(spark.sparkContext.hadoopConfiguration, outputLocation)
  }

  override def afterAll() : Unit = {
    super.afterAll()
    EmbeddedPostgresql.close()
    new HadoopFileUtil().delete(spark.sparkContext.hadoopConfiguration, outputLocation)
    spark.close()
  }

  "AssessmentArchivalJob" should "archive the batch which is not archived in past" in {
    implicit val fc = new FrameworkContext()
    val batchId = "batch-011"
    val courseId = "do_1130928636168192001667"

    val strConfig= """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.$job_name","modelParams":{"mode":"archival","request":{"archivalTable":"assessment_aggregator","batchId":"batch-011","collectionId": "do_1130928636168192001667","date":"2021-11-01"},"blobConfig":{"store":"azure","blobExt":"csv.gz","reportPath":"assessment-archived-data/","container":"reports"},"sparkCassandraConnectionHost":"{{ core_cassandra_host }}","fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')"},"parallelization":8,"appName":"$job_name"}"""
    implicit val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)

    AssessmentArchivalJob.execute()

    val batch011Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/${batchId}_${courseId}/2021*.csv.gz")

    batch011Results.count() should be (5)

    val user1 = batch011Results.filter(col("user_id") === "user-001")
    user1.count() should be (2)

    val user1attempt1 = user1.filter(col("attempt_id") === "attempt-001").first
    user1attempt1.getAs[String]("course_id") should be ("do_1130928636168192001667")
    user1attempt1.getAs[String]("content_id") should be ("do_1128870328040161281204")
    user1attempt1.getAs[String]("last_attempted_on") should be ("1638357693200")
    user1attempt1.getAs[String]("grand_total") should be ("10")
    user1attempt1.getAs[String]("total_max_score") should be ("10.0")
    user1attempt1.getAs[String]("total_score") should be ("10.0")
    user1attempt1.getAs[String]("question") should be ("[]")
    user1attempt1.getAs[String]("updated_on") should be ("1638357693000")

    val user1attempt2 = user1.filter(col("attempt_id") === "attempt-002").first

    user1attempt2.getAs[String]("course_id") should be ("do_1130928636168192001667")
    user1attempt2.getAs[String]("content_id") should be ("do_1128870328040161281204")
    user1attempt2.getAs[String]("last_attempted_on") should be (null)
    user1attempt2.getAs[String]("grand_total") should be ("20")
    user1attempt2.getAs[String]("total_max_score") should be ("20.0")
    user1attempt2.getAs[String]("total_score") should be ("20.0")
    val questionsList = JSONUtils.deserialize[List[Map[String, AnyRef]]](user1attempt2.getAs[String]("question"))
    questionsList.size should be (4)

    user1attempt2.getAs[String]("updated_on") should be ("1639052254823")

    val user2Result = batch011Results.filter(col("user_id") === "user-002")
    user2Result.count() should be (1)

    val user3Result = batch011Results.filter(col("user_id") === "user-003")
    user3Result.count() should be (2)

    val archivalRequests = AssessmentArchivalJob.getRequests(AssessmentArchivalJob.jobId, None)
    archivalRequests.size should be (2)

    archivalRequests.map(ar => ar.request_id).toList should contain allElementsOf List("2A04B5AF40E2E249EBB63530F19656F7", "AC0F439E287263DB49D54004DAA4644B")
    archivalRequests.map(ar => ar.batch_id).toList.distinct should contain allElementsOf List("batch-011")
    archivalRequests.map(ar => ar.collection_id).toList.distinct should contain allElementsOf List("do_1130928636168192001667")
    archivalRequests.map(ar => ar.archival_status).toList.distinct should contain allElementsOf List("SUCCESS")
    archivalRequests.map(ar => ar.blob_url.get).toList.head.head should include (s"src/test/resources/reports/assessment-archived-data/${batchId}_${courseId}/2021")
    archivalRequests.map(ar => ar.iteration.get).toList.distinct should contain allElementsOf List(0)
    archivalRequests.map(ar => ar.err_message.get).toList.distinct should contain allElementsOf List("")
  }

  it should "archive the multiple batches which is not archived in past" in {
    implicit val fc = new FrameworkContext()
    val batchId = "batch-011"
    val courseId = "do_1130928636168192001667"

    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.$job_name","modelParams":{"mode":"archival","request":{"archivalTable":"assessment_aggregator","batchFilters":["batch-011", "batch-021"],"date":"2021-11-01"},"blobConfig":{"store":"azure","blobExt":"csv.gz","reportPath":"assessment-archived-data/","container":"reports"},"sparkCassandraConnectionHost":"{{ core_cassandra_host }}","fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')"},"parallelization":8,"appName":"$job_name"}"""
    implicit val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)

    AssessmentArchivalJob.execute()

    val archivalRequests = AssessmentArchivalJob.getRequests(AssessmentArchivalJob.jobId, None)
    archivalRequests.size should be (3)

    archivalRequests.map(ar => ar.batch_id).toList.distinct should contain allElementsOf List("batch-011", "batch-021")
  }

  it should "archive the batch which is failed to archive in past" in {
    implicit val fc = new FrameworkContext()
    val batchId = "batch-011"
    val courseId = "do_1130928636168192001667"

    EmbeddedPostgresql.execute("INSERT INTO archival_metadata (request_id, batch_id, collection_id , resource_type , job_id , archival_date, completion_date, archival_status, blob_url, iteration,request_data , err_message ) VALUES ('2A04B5AF40E2E249EBB63530F19656F7', 'batch-011', 'do_1130928636168192001667', 'assessment', 'assessment-archival','2021-12-09 05:58:18.666', null,'FAILED', null, 1,'{\"batchId\": \"batch-011\", \"week\": 48, \"year\": 2021}', NULL);")

    val strConfig= """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.$job_name","modelParams":{"mode":"archival","request":{"archivalTable":"assessment_aggregator","batchId":"batch-011","collectionId": "do_1130928636168192001667","date":"2021-11-01"},"blobConfig":{"store":"azure","blobExt":"csv.gz","reportPath":"assessment-archived-data/","container":"reports"},"sparkCassandraConnectionHost":"{{ core_cassandra_host }}","fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')"},"parallelization":8,"appName":"$job_name"}"""
    implicit val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)

    AssessmentArchivalJob.execute()

    val batch011Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/${batchId}_${courseId}/2021*.csv.gz")

    batch011Results.count() should be (5)

    val user1 = batch011Results.filter(col("user_id") === "user-001")
    user1.count() should be (2)

    val user2Result = batch011Results.filter(col("user_id") === "user-002")
    user2Result.count() should be (1)

    val user3Result = batch011Results.filter(col("user_id") === "user-003")
    user3Result.count() should be (2)

    val archivalRequests = AssessmentArchivalJob.getRequests(AssessmentArchivalJob.jobId, Option(batchId))
    archivalRequests.size should be (2)

    val failedRequest = AssessmentArchivalJob.getRequest(AssessmentArchivalJob.jobId, "do_1130928636168192001667", batchId, List(2021, 48))

    failedRequest.request_id should be ("AC0F439E287263DB49D54004DAA4644B")
    failedRequest.archival_status should be ("SUCCESS")
    failedRequest.blob_url.get.head should include (s"src/test/resources/reports/assessment-archived-data/${batchId}_${courseId}/2021")
  }

  it should "skip archival for the batch which is archived in past" in {
    implicit val fc = new FrameworkContext()
    val batchId = "batch-011"
    val courseId = "do_1130928636168192001667"

    EmbeddedPostgresql.execute("INSERT INTO archival_metadata (request_id, batch_id, collection_id , resource_type , job_id , archival_date, completion_date, archival_status, blob_url, iteration,request_data , err_message ) VALUES ('949887DE6364A07AE1BB5A04504368F9', 'batch-011', 'do_1130928636168192001667', 'assessment', 'assessment-archival','2021-12-09 05:58:18.666', null,'SUCCESS', '{\"reports/assessment-archival/batch-011/2021-48.csv.gz\"}', 1,'{\"batchId\": \"batch-011\", \"week\": 48, \"year\": 2021}', NULL);")

    val strConfig= """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.$job_name","modelParams":{"mode":"archival","request":{"archivalTable":"assessment_aggregator","batchId":"batch-011","collectionId": "do_1130928636168192001667","date":"2021-11-01"},"blobConfig":{"store":"azure","blobExt":"csv.gz","reportPath":"assessment-archived-data/","container":"reports"},"sparkCassandraConnectionHost":"{{ core_cassandra_host }}","fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')"},"parallelization":8,"appName":"$job_name"}"""
    implicit val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)

    AssessmentArchivalJob.execute()

    val batch011Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/${batchId}_${courseId}/2021*.csv.gz")

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

    val archivalRequests = AssessmentArchivalJob.getRequests(AssessmentArchivalJob.jobId, Option(batchId))
    archivalRequests.size should be (2)
  }

  it should "delete the archived records based on blob files" in {
    implicit val fc = new FrameworkContext()
    val batchId = "batch-011"
    val courseId = "do_1130928636168192001667"

    val strConfig= """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.$job_name","modelParams":{"mode":"archival","request":{"archivalTable":"assessment_aggregator","batchId":"batch-011","collectionId": "do_1130928636168192001667","date":"2021-11-01"},"blobConfig":{"store":"azure","blobExt":"csv.gz","reportPath":"assessment-archived-data/","container":"reports"},"sparkCassandraConnectionHost":"{{ core_cassandra_host }}","fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')"},"parallelization":8,"appName":"$job_name"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)

    AssessmentArchivalJob.execute()(spark, fc, jobConfig)

    val batch011Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/${batchId}_${courseId}/2021*.csv.gz")

    batch011Results.count() should be (5)

    val cassData = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "assessment_aggregator", "keyspace" -> "sunbird_courses")).load()

    cassData.filter(col("batch_id") === batchId).count() should be (5)

    val archivalRequests = AssessmentArchivalJob.getRequests(AssessmentArchivalJob.jobId, Option(batchId))
    archivalRequests.size should be (2)

    archivalRequests.map(ar => ar.archival_status).toList.distinct should contain allElementsOf List("SUCCESS")
    archivalRequests.map(ar => ar.deletion_status).toList.distinct should contain allElementsOf List(null)

    val delStrConfig= """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.$job_name","modelParams":{"mode":"delete","request":{"archivalTable":"assessment_aggregator","batchId":"batch-011","collectionId": "do_1130928636168192001667","date":"2021-11-01"},"blobConfig":{"store":"local","blobExt":"csv.gz","reportPath":"src/test/resources/reports/assessment-archived-data/","container":"reports"},"sparkCassandraConnectionHost":"{{ core_cassandra_host }}","fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')"},"parallelization":8,"appName":"$job_name"}"""

    val delJobConfig = JSONUtils.deserialize[JobConfig](delStrConfig)

    AssessmentArchivalJob.execute()(spark, fc, delJobConfig)

    val delCassData = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "assessment_aggregator", "keyspace" -> "sunbird_courses")).load()

    delCassData.filter(col("batch_id") === batchId).count() should be (0)

    val deletionRequests = AssessmentArchivalJob.getRequests(AssessmentArchivalJob.jobId, Option(batchId))
    deletionRequests.map(ar => ar.archival_status).toList.distinct should contain allElementsOf List("SUCCESS")
    deletionRequests.map(ar => ar.deletion_status).toList.distinct should contain allElementsOf List("SUCCESS")
  }

  it should "not delete the records the if the blob file is not available" in {
    implicit val fc = new FrameworkContext()
    val batchId = "batch-011"
    val courseId = "do_1130928636168192001667"

    // Week 48 records are processed will not be processed for archival again
    EmbeddedPostgresql.execute("INSERT INTO archival_metadata (request_id, batch_id, collection_id , resource_type , job_id , archival_date, completion_date, archival_status, blob_url, iteration,request_data , err_message ) VALUES ('AC0F439E287263DB49D54004DAA4644B', 'batch-011', 'do_1130928636168192001667', 'assessment', 'assessment-archival','2021-12-09 05:58:18.666', null,'SUCCESS', '{\"reports/assessment-archival/batch-011/2021-48.csv.gz\"}', 1,'{\"batchId\": \"batch-011\", \"week\": 48, \"year\": 2021}', NULL);")

    val strConfig= """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.$job_name","modelParams":{"mode":"archival","request":{"archivalTable":"assessment_aggregator","batchId":"batch-011","collectionId": "do_1130928636168192001667","date":"2021-11-01"},"blobConfig":{"store":"azure","blobExt":"csv.gz","reportPath":"assessment-archived-data/","container":"reports"},"sparkCassandraConnectionHost":"{{ core_cassandra_host }}","fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')"},"parallelization":8,"appName":"$job_name"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)

    AssessmentArchivalJob.execute()(spark, fc, jobConfig)

    val batch011Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/${batchId}_${courseId}/2021*.csv.gz")

    batch011Results.count() should be (3)

    val delStrConfig= """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.$job_name","modelParams":{"mode":"delete","request":{"archivalTable":"assessment_aggregator","batchId":"batch-011","collectionId": "do_1130928636168192001667","date":"2021-11-01"},"blobConfig":{"store":"local","blobExt":"csv.gz","reportPath":"src/test/resources/reports/assessment-archived-data/","container":"reports"},"sparkCassandraConnectionHost":"{{ core_cassandra_host }}","fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')"},"parallelization":8,"appName":"$job_name"}"""

    val delJobConfig = JSONUtils.deserialize[JobConfig](delStrConfig)

    AssessmentArchivalJob.execute()(spark, fc, delJobConfig)

    val delCassData = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "assessment_aggregator", "keyspace" -> "sunbird_courses")).load()

    delCassData.filter(col("batch_id") === batchId).count() should be (2)

    val skippedRequest = AssessmentArchivalJob.getRequest(AssessmentArchivalJob.jobId, "do_1130928636168192001667", batchId, List(2021, 48))

    skippedRequest.request_id should be ("AC0F439E287263DB49D54004DAA4644B")
    skippedRequest.archival_status should be ("SUCCESS")
    skippedRequest.deletion_status should be ("FAILED")
    skippedRequest.err_message.get should include("Path does not exist")

    val deletionRequest = AssessmentArchivalJob.getRequest(AssessmentArchivalJob.jobId, "do_1130928636168192001667", batchId, List(2021, 49))

    deletionRequest.request_id should be ("2A04B5AF40E2E249EBB63530F19656F7")
    deletionRequest.archival_status should be ("SUCCESS")
    deletionRequest.deletion_status should be ("SUCCESS")

  }

  it should "Archive all batches if neither batchid nor batchfilters present" in {
    implicit val fc = new FrameworkContext()

    val strConfig= """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.$job_name","modelParams":{"mode":"archival","request":{"archivalTable":"assessment_aggregator","date":"2021-11-01"},"blobConfig":{"store":"azure","blobExt":"csv.gz","reportPath":"assessment-archived-data/","container":"reports"},"sparkCassandraConnectionHost":"{{ core_cassandra_host }}","fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')"},"parallelization":8,"appName":"$job_name"}"""
    implicit val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)

    AssessmentArchivalJob.execute()

    val archivalRequests = AssessmentArchivalJob.getRequests(AssessmentArchivalJob.jobId, None)

    archivalRequests.map(ar => ar.archival_status).toList.distinct should contain allElementsOf List("SUCCESS")

    archivalRequests.map(ar => ar.batch_id).toList.distinct should contain allElementsOf List("batch-011", "batch-021", "batch-031")
  }

}
