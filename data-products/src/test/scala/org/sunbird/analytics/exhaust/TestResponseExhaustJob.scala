package org.sunbird.analytics.exhaust

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{HadoopFileUtil, JSONUtils}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.joda.time.DateTimeZone
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.exhaust.collection.ResponseExhaustJob
import org.sunbird.analytics.job.report.BaseReportSpec
import org.sunbird.analytics.util.{EmbeddedCassandra, EmbeddedPostgresql, RedisCacheUtil}
import redis.clients.jedis.Jedis
import redis.embedded.RedisServer

import scala.collection.JavaConverters._


case class ResponseExhaustReport(`Collection Id`: String, `Collection Name`: String, `Batch Id`: String, `Batch Name`: String, `User UUID`: String,
                                 `QuestionSet Id`: String, `QuestionSet Title`: String, `Attempt Id`: String, `Attempted On`: String, `Question Id` : String,
                                 `Question Type`: String, `Question Title`: String, `Question Description`: String, `Question Duration`: String, `Question Score`: String,
                                 `Question Max Score`: String, `Question Options`: String, `Question Response`: String)

class TestResponseExhaustJob extends BaseReportSpec with MockFactory with BaseReportsJob {

  val jobRequestTable = "job_request"
  implicit var spark: SparkSession = _
  var redisServer: RedisServer = _
  var jedis: Jedis = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession();
    redisServer = new RedisServer(6379)
    // redis setup
    println("redis is active? response: " + redisServer.isActive)
    if(!redisServer.isActive) {
      redisServer.start();
    }
    val redisConnect = new RedisCacheUtil("127.0.0.1", 6379)
    jedis = redisConnect.getConnection(0)
    setupRedisData(jedis)
    // embedded cassandra setup
    EmbeddedCassandra.loadData("src/test/resources/exhaust/report_data.cql") // Load test data in embedded cassandra server
    // embedded postgres setup
    EmbeddedPostgresql.start()
    EmbeddedPostgresql.createJobRequestTable()
  }

  override def afterAll() : Unit = {
    super.afterAll();
    redisServer.stop();
    println("redis server closed? response: " + redisServer.isActive)
    EmbeddedCassandra.close()
    EmbeddedPostgresql.close()
  }

  def setupRedisData(jedis: Jedis): Unit = {
    jedis.hmset("user:user-001", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Manju", "userid": "user-001", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "01250894314817126443", "email": "manju@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-002", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Mahesh", "userid": "user-002", "state": "Andhra Pradesh", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "01285019302823526477", "email": "mahesh@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-003", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Sowmya", "userid": "user-003", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "01250894314817126443", "email": "sowmya@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-004", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Utkarsha", "userid": "user-004", "state": "Delhi", "district": "babarpur", "userchannel": "sunbird-dev", "rootorgid": "01250894314817126443", "email": "utkarsha@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-005", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Isha", "userid": "user-005", "state": "MP", "district": "Jhansi", "userchannel": "sunbird-dev", "rootorgid": "01250894314817126443", "email": "isha@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-006", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Revathi", "userid": "user-006", "state": "Andhra Pradesh", "district": "babarpur", "userchannel": "sunbird-dev", "rootorgid": "01250894314817126443", "email": "revathi@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-007", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Sunil", "userid": "user-007", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "0126391644091351040", "email": "sunil@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-008", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Anoop", "userid": "user-008", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "anoop@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-009", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Kartheek", "userid": "user-009", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "01285019302823526477", "email": "kartheekp@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-010", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Anand", "userid": "user-010", "state": "Tamil Nadu", "district": "Chennai", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "anandp@ilimi.in", "usersignintype": "Validated"};"""))

    jedis.close()
  }

  "TestResponseExhaustJob" should "generate final output as csv and zip files" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration) VALUES ('do_1131350140968632321230_batch-001:01250894314817126443', '37564CF8F134EE7532F125651B51D17F', 'response-exhaust', 'SUBMITTED', '{\"batchId\": \"batch-001\"}', 'user-002', 'b00bc992ef25f1a9a8d63291e20efc8d', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0);")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.ProgressExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"localhost","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"12","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"", "storageContainer": ""},"parallelization":8,"appName":"Progress Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    val jobResponse = ResponseExhaustJob.execute()

    val outputLocation = AppConf.getConfig("collection.exhaust.store.prefix")
    val outputDir = "response-exhaust"
    val batch1 = "batch-001"
    val filePath = getFilePath(batch1)
    val jobName = ResponseExhaustJob.jobName()

    implicit val responseExhaustEncoder = Encoders.product[ResponseExhaustReport]
    val batch1Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/$filePath.csv").as[ResponseExhaustReport].collectAsList().asScala
    batch1Results.size should be (6)

    val user1Result = batch1Results.filter(f => f.`User UUID`.equals("user-001"))
    user1Result.map(f => f.`Collection Id`).toList should contain atLeastOneElementOf List("do_1130928636168192001667")
    user1Result.map(f => f.`Batch Id`).toList should contain atLeastOneElementOf List("BatchId_batch-001")
    user1Result.map(f => f.`User UUID`).toList should contain atLeastOneElementOf List("user-001")
    user1Result.map(f => f.`Attempt Id`).toList should contain atLeastOneElementOf List("attempat-001")
    user1Result.map(f => f.`QuestionSet Id`).toList should contain atLeastOneElementOf List("do_1128870328040161281204", "do_112876961957437440179")
    user1Result.map(f => f.`QuestionSet Title`).toList should contain atLeastOneElementOf List("SelfAssess for course", "Assessment score report using summary plugin")
    user1Result.map(f => f.`Question Id`).toList should contain theSameElementsAs List("do_213019475454476288155", "do_213019970118279168165", "do_213019972814823424168", "do_2130256513760624641171")
    user1Result.map(f => f.`Question Type`).toList should contain theSameElementsAs List("mcq", "mcq", "mtf", "mcq")
    user1Result.map(f => f.`Question Title`).toList should contain atLeastOneElementOf List("testQuestiontextandformula", "test with formula")
    user1Result.map(f => f.`Question Description`).toList should contain atLeastOneElementOf  List("testQuestiontextandformula")
    user1Result.map(f => f.`Question Duration`).toList should contain theSameElementsAs List("1", "1", "2", "12")
    user1Result.map(f => f.`Question Score`).toList should contain theSameElementsAs List("1.0", "1.0", "0.33", "10.0")
    user1Result.map(f => f.`Question Max Score`).toList should contain theSameElementsAs List("1.0", "1.0", "1.0", "10.0")
    user1Result.map(f => f.`Question Options`).head should be ("""[{'1':'{'text':'A=pi r^2'}'},{'2':'{'text':'no'}'},{'answer':'{'correct':['1']}'}]""")
    user1Result.map(f => f.`Question Response`).head should be ("""[{'1':'{'text':'A=pi r^2'}'}]""")

    val pResponse = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='response-exhaust'")

    while(pResponse.next()) {
      pResponse.getString("err_message") should be ("")
      pResponse.getString("dt_job_submitted") should be ("2020-10-19 05:58:18.666")
      pResponse.getString("download_urls") should be (s"{reports/response-exhaust/batch-001_response_${getDate()}.zip}")
      pResponse.getString("dt_file_created") should be (null)
      pResponse.getString("iteration") should be ("0")
    }

    new HadoopFileUtil().delete(spark.sparkContext.hadoopConfiguration, outputLocation)
  }

  it should "execute main method" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration) VALUES ('do_1131350140968632321230_batch-001:01250894314817126443', '37564CF8F134EE7532F125651B51D17F', 'response-exhaust', 'SUBMITTED', '{\"batchId\": \"batch-001\"}', 'user-002', 'b00bc992ef25f1a9a8d63291e20efc8d', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0);")

    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.ResponseExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"localhost","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"12","sparkCassandraConnectionHost":"localhost", "sparkUserDbRedisPort": "6379","fromDate":"","toDate":"", "storageContainer": ""},"parallelization":8,"appName":"Response Exhaust"}"""
    ResponseExhaustJob.main(strConfig)

    val outputLocation = AppConf.getConfig("collection.exhaust.store.prefix")
    new HadoopFileUtil().delete(spark.sparkContext.hadoopConfiguration, outputLocation)
  }

  def getFilePath(batchId: String)(implicit config: JobConfig): String = {
    ResponseExhaustJob.getReportPath() + batchId + "_" + ResponseExhaustJob.getReportKey() + "_" + getDate()
  }

  def getDate(): String = {
    val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
    dateFormat.print(System.currentTimeMillis());
  }
}