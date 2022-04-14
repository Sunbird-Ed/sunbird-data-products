package org.sunbird.analytics.exhaust

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Encoders, SparkSession}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{HadoopFileUtil, JSONUtils}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.exhaust.collection.{AssessmentData, ProgressExhaustJobV2}
import org.sunbird.analytics.util.{BaseSpec, EmbeddedCassandra, EmbeddedPostgresql, RedisConnect}
import redis.embedded.RedisServer

import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.JavaConverters._

class TestProgressExhaustJobV2 extends BaseSpec with MockFactory with BaseReportsJob {

  val jobRequestTable = "job_request"
  implicit var spark: SparkSession = _

  var redisServer: RedisServer = _

  override def beforeAll(): Unit = {
    spark = getSparkSession();
    super.beforeAll()
    redisServer = new RedisServer(6341)
    redisServer.start()
    setupRedisData()
    EmbeddedCassandra.loadData("src/test/resources/exhaust/progress-exhaust-v2.cql") // Load test data in embedded cassandra server
    EmbeddedPostgresql.start()
    EmbeddedPostgresql.createJobRequestTable()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    redisServer.stop()
    println("******** closing the redis connection **********" + redisServer.isActive)
    EmbeddedCassandra.close()
    EmbeddedPostgresql.close()
    spark.close()
  }

  def setupRedisData(): Unit = {
    val redisConnect = new RedisConnect("localhost", 6341)
    val jedis = redisConnect.getConnection(0, 100000)
    jedis.hmset("user:user-001", JSONUtils.deserialize[java.util.Map[String, String]]("""{"cluster":"CLUSTER1","firstname":"Manju","subject":"[\"IRCS\"]","schooludisecode":"3183211","usertype":"administrator","usersignintype":"Validated","language":"[\"English\"]","medium":"[\"English\"]","userid":"a962a4ff-b5b5-46ad-a9fa-f54edf1bcccb","schoolname":"DPS, MATHURA","rootorgid":"01250894314817126443","lastname":"Kapoor","framework":"[\"igot_health\"]","orgname":"Root Org2","phone":"","usersubtype":"deo","district":"bengaluru","grade":"[\"Volunteers\"]","block":"BLOCK1","state":"Karnataka","board":"[\"IGOT-Health\"]","email":""};"""))
    jedis.hmset("user:user-002", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Mahesh", "userid": "user-002", "state": "Andhra Pradesh", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "mahesh@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-003", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Sowmya", "userid": "user-003","usertype":"administrator", "usersubtype":"deo", "cluster": "anagha" ,"state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "sowmya@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-004", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Utkarsha", "userid": "user-004", "state": "Delhi", "district": "babarpur", "userchannel": "sunbird-dev", "rootorgid": "01250894314817126443", "email": "utkarsha@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-005", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Isha", "userid": "user-005", "state": "MP", "district": "Jhansi", "userchannel": "sunbird-dev", "rootorgid": "01250894314817126443", "email": "isha@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-006", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Revathi", "userid": "user-006", "state": "Andhra Pradesh", "district": "babarpur", "userchannel": "sunbird-dev", "rootorgid": "01250894314817126443", "email": "revathi@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-007", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Sunil", "userid": "user-007", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "0126391644091351040", "email": "sunil@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-008", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Anoop", "userid": "user-008", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "anoop@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-009", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Kartheek", "userid": "user-009", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "01285019302823526477", "email": "kartheekp@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-010", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Anand", "userid": "user-010", "state": "Tamil Nadu", "district": "Chennai", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "anandp@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-011", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Santhosh", "userid": "user-011", "state": "Tamil Nadu", "district": "Chennai", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "anandp@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-012", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Rayulu", "userid": "user-012","usertype":"administrator", "usersubtype":"deo", "cluster": "anagha", "state": "Tamil Nadu", "district": "Chennai", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "anandp@ilimi.in", "usersignintype": "Validated"};"""))

    jedis.close()
  }

  it should "generate the report with all the correct data" in {

    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1130928636168192001667_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F', 'progress-exhaust', 'SUBMITTED', '{\"batchId\": \"batch-001\"}', 'user-002', 'b00bc992ef25f1a9a8d63291e20efc8d', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.ProgressExhaustJobV2","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"{{ sunbird_es_host }}","sparkRedisConnectionHost":"localhost","sparkUserDbRedisPort":6341,"sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"Progress Exhaust V2"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig
    ProgressExhaustJobV2.execute()

    val outputLocation = AppConf.getConfig("collection.exhaust.store.prefix")
    val outputDir = "progress-exhaust"
    val batch1 = "batch-001"
    val requestId = "37564CF8F134EE7532F125651B51D17F"
    val filePath = ProgressExhaustJobV2.getFilePath(batch1, requestId)
    val jobName = ProgressExhaustJobV2.jobName()

    implicit val responseExhaustEncoder = Encoders.product[ProgressExhaustReport]
    val batchResult = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/$filePath.csv")

    val filteredDF = batchResult.toDF().filter(col("Collection Id") === "do_1130928636168192001667"
      && col("Batch Id") === "BatchId_batch-001" && col("User UUID") === "user-001")
    import org.apache.spark.sql.SparkSession
    val spark1 = SparkSession.builder.getOrCreate
    import spark1.implicits._

    filteredDF.select("progress").rdd.map(r => r(0)).collect.toList.head should be("100")
    filteredDF.select("Total Score").rdd.map(r => r(0)).collect.toList.head should be("100%")
    filteredDF.select("do_1128870328040161281204 - Score").rdd.map(r => r(0)).collect.toList.head should be("100%")

    val batch1Results = batchResult.as[ProgressExhaustReport].collectAsList().asScala
    batch1Results.size should be(4)
    batch1Results.map(f => f.`Collection Id`).toList should contain atLeastOneElementOf List("do_1130928636168192001667")
    batch1Results.map(f => f.`Collection Name`).toList should contain atLeastOneElementOf List("24 aug course")
    batch1Results.map(f => f.`Batch Id`).toList should contain atLeastOneElementOf List("BatchId_batch-001")
    batch1Results.map(f => f.`Batch Name`).toList should contain atLeastOneElementOf List("Basic Java")
    batch1Results.map { res => res.`User UUID` }.toList should contain theSameElementsAs List("user-001", "user-002", "user-003", "user-004")
    batch1Results.map { res => res.`State` }.toList should contain theSameElementsAs List("Karnataka", "Andhra Pradesh", "Karnataka", "Delhi")
    batch1Results.map { res => res.`District` }.toList should contain theSameElementsAs List("bengaluru", "bengaluru", "bengaluru", "babarpur")
    batch1Results.map(f => f.`Enrolment Date`).toList should contain allElementsOf List("15/11/2019")
    batch1Results.map(f => f.`Completion Date`).toList should contain allElementsOf List(null)
    batch1Results.map(f => f.`Progress`).toList should contain allElementsOf List("100")
    batch1Results.map(f => f.`Cluster Name`).toList should contain atLeastOneElementOf List("CLUSTER1")
    batch1Results.map(f => f.`User Type`).toList should contain atLeastOneElementOf List("administrator")
    batch1Results.map(f => f.`User Sub Type`).toList should contain atLeastOneElementOf List("deo")
    //
    val pResponse = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='progress-exhaust'")
    val reportDate = getDate("yyyyMMdd").format(Calendar.getInstance().getTime)

    while (pResponse.next()) {
      pResponse.getString("status") should be("SUCCESS")
      pResponse.getString("err_message") should be("")
      pResponse.getString("dt_job_submitted") should be("2020-10-19 05:58:18.666")
      pResponse.getString("download_urls") should be(s"""{reports/progress-exhaust/$requestId/batch-001_progress_${reportDate}.zip}""")
      pResponse.getString("dt_file_created") should be(null)
      pResponse.getString("iteration") should be("0")
    }

    new HadoopFileUtil().delete(spark.sparkContext.hadoopConfiguration, outputLocation)

    //Test coverage for filterAssessmentsFromHierarchy method
    val assessmentData = ProgressExhaustJobV2.filterAssessmentsFromHierarchy(List(), Map(), AssessmentData("do_1130928636168192001667", List()))
    assessmentData.courseid should be("do_1130928636168192001667")
    assert(assessmentData.assessmentIds.isEmpty)

  }

  it should "make request as failed and add error message for invalid request_data" in {

    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    // batchid or batchfilter should present
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1130928636168192001667_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F', 'progress-exhaust', 'SUBMITTED', '{\"batchFilter\": \" \"}', 'user-002', 'b00bc992ef25f1a9a8d63291e20efc8d', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")

    implicit val fc = new FrameworkContext()

    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.ProgressExhaustJobV2","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"{{ sunbird_es_host }}","sparkRedisConnectionHost":"localhost","sparkUserDbRedisPort":6341,"sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"Progress Exhaust V2"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig
    ProgressExhaustJobV2.execute()

    val pResponse = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='progress-exhaust'")
    val reportDate = getDate("yyyyMMdd").format(Calendar.getInstance().getTime)

    while (pResponse.next()) {
      pResponse.getString("request_id") should be("37564CF8F134EE7532F125651B51D17F")
      pResponse.getString("status") should be("FAILED")
      pResponse.getString("err_message") should include ("Internal Server Error:")
    }
  }

  it should "validate the report path" in {
    val batch1 = "batch-001"
    val requestId = "37564CF8F134EE7532F125651B51D17F"
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.ProgressExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"{{ sunbird_es_host }}","sparkRedisConnectionHost":"localhost","sparkUserDbRedisPort":6341,"sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"Progress Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig
    val onDemandModeFilepath = ProgressExhaustJobV2.getFilePath(batch1, requestId)
    val reportDate = getDate("yyyyMMdd").format(Calendar.getInstance().getTime())
    onDemandModeFilepath should be(s"progress-exhaust/$requestId/batch-001_progress_$reportDate")

    val standAloneModeFilePath = ProgressExhaustJobV2.getFilePath(batch1, "")
    standAloneModeFilePath should be(s"progress-exhaust/batch-001_progress_$reportDate")
  }

  def getDate(pattern: String): SimpleDateFormat = {
    new SimpleDateFormat(pattern)
  }


  it should "Generate a report for StandAlone Mode" in {
    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.ProgressExhaustJobV2","modelParams":{"store":"local","mode":"standalone","batchFilters":["TPD"],"searchFilter":{"request":{"filters":{"status":["Live"],"contentType":"Course"},"fields":["identifier","name","organisation","channel"],"limit":10}},"sparkElasticsearchConnectionHost":"{{ sunbird_es_host }}","sparkRedisConnectionHost":"localhost","sparkUserDbRedisPort":6341,"sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"Progress Exhaust Job V2"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig
    ProgressExhaustJobV2.execute()
    val outputLocation = AppConf.getConfig("collection.exhaust.store.prefix")
    val batch1 = "batch-001"
    val filePath = ProgressExhaustJobV2.getFilePath(batch1, "")
    implicit val responseExhaustEncoder = Encoders.product[ProgressExhaustReport]
    val batch1Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/$filePath.csv").as[ProgressExhaustReport].collectAsList().asScala


    batch1Results.size should be(4)
    batch1Results.map(f => f.`Collection Id`).toList should contain atLeastOneElementOf List("do_1130928636168192001667")
    batch1Results.map(f => f.`Collection Name`).toList should contain atLeastOneElementOf List("24 aug course")
    batch1Results.map(f => f.`Batch Id`).toList should contain atLeastOneElementOf List("BatchId_batch-001")
    batch1Results.map(f => f.`Batch Name`).toList should contain atLeastOneElementOf List("Basic Java")
    batch1Results.map { res => res.`User UUID` }.toList should contain theSameElementsAs List("user-001", "user-002", "user-003", "user-004")
    batch1Results.map { res => res.`State` }.toList should contain theSameElementsAs List("Karnataka", "Andhra Pradesh", "Karnataka", "Delhi")
    batch1Results.map { res => res.`District` }.toList should contain theSameElementsAs List("bengaluru", "bengaluru", "bengaluru", "babarpur")
    batch1Results.map(f => f.`Enrolment Date`).toList should contain allElementsOf List("15/11/2019")
    batch1Results.map(f => f.`Completion Date`).toList should contain allElementsOf List(null)
    batch1Results.map(f => f.`Progress`).toList should contain allElementsOf List("100")
    batch1Results.map(f => f.`Cluster Name`).toList should contain atLeastOneElementOf List("CLUSTER1")
    batch1Results.map(f => f.`User Type`).toList should contain atLeastOneElementOf List("administrator")
    batch1Results.map(f => f.`User Sub Type`).toList should contain atLeastOneElementOf List("deo")
  }
}
