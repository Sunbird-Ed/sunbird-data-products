package org.sunbird.analytics.exhaust

import org.apache.spark.sql.{Encoders, SparkSession}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{HadoopFileUtil, JSONUtils}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig, StorageConfig}
import org.joda.time.DateTimeZone
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.exhaust.collection.{UDFUtils, UserInfoExhaustJob}
import org.sunbird.analytics.job.report.BaseReportSpec
import org.sunbird.analytics.util.{EmbeddedCassandra, EmbeddedPostgresql, RedisCacheUtil}
import redis.clients.jedis.Jedis
import redis.embedded.RedisServer

import scala.collection.JavaConverters._


case class UserInfoExhaustReport(`Collection Id`: String, `Collection Name`: String, `Batch Id`: String, `Batch Name`: String, `User UUID`: String, `User Name`: String, `State`: String, `District`: String, `Org Name`: String, `External ID`: String,
                                 `Email ID`: String, `Mobile Number`: String, `Consent Provided`: String, `Consent Provided Date`: String)

class TestUserInfoExhaustJob extends BaseReportSpec with MockFactory with BaseReportsJob {

  val jobRequestTable = "job_request"
  implicit var spark: SparkSession = _
  var redisServer: RedisServer = _
  var jedis: Jedis = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession();

    redisServer = new RedisServer(6379)
    // redis setup
    println("redis is active? userinfo: " + redisServer.isActive)
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
    println("jedis instance connected or not?: userinfo " + jedis.isConnected)
    jedis.shutdown()
    redisServer.stop();
    println("jedis instance closed or not?: userinfo " + jedis.isConnected)
    println("redis server closed? userinfo: " + redisServer.isActive)
    EmbeddedCassandra.close()
    EmbeddedPostgresql.close()
  }

  def setupRedisData(jedis: Jedis): Unit = {
    jedis.hmset("user:user-001", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Manju", "userid": "user-001","orgname": "Pre-prod Custodian Organization", "externalid": "1234", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "01250894314817126443", "email": "manju@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-002", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Mahesh", "userid": "user-002","orgname": "Pre-prod Custodian Organization", "externalid": "1234", "state": "Andhra Pradesh", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "mahesh@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-003", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Sowmya", "userid": "user-003","orgname": "Pre-prod Custodian Organization", "externalid": "1234", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "sowmya@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-004", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Utkarsha", "userid": "user-004","orgname": "Pre-prod Custodian Organization", "externalid": "1234", "state": "Delhi", "district": "babarpur", "userchannel": "sunbird-dev", "rootorgid": "01250894314817126443", "email": "utkarsha@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-005", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Isha", "userid": "user-005", "state": "MP", "district": "Jhansi", "userchannel": "sunbird-dev", "rootorgid": "01250894314817126443", "email": "isha@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-006", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Revathi", "userid": "user-006", "state": "Andhra Pradesh", "district": "babarpur", "userchannel": "sunbird-dev", "rootorgid": "01250894314817126443", "email": "revathi@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-007", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Sunil", "userid": "user-007", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "0126391644091351040", "email": "sunil@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-008", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Anoop", "userid": "user-008", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "anoop@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-009", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Kartheek", "userid": "user-011", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "01285019302823526477", "email": "kartheekp@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-010", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Anand", "userid": "user-012", "state": "Tamil Nadu", "district": "Chennai", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "anandp@ilimi.in", "usersignintype": "Validated"};"""))

    jedis.close()
  }

  "UserInfoExhaustJob" should "generate the user info report with all the users for a batch" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1131350140968632321230_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F', 'userinfo-exhaust', 'SUBMITTED', '{\"batchId\": \"batch-001\"}', 'user-002', 'b00bc992ef25f1a9a8d63291e20efc8d', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.UserInfoExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"localhost","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"12","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"UserInfo Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    UserInfoExhaustJob.execute()

    val outputLocation = AppConf.getConfig("collection.exhaust.store.prefix")
    val outputDir = "response-exhaust"
    val batch1 = "batch-001"
    val filePath = UserInfoExhaustJob.getFilePath(batch1)
    val jobName = UserInfoExhaustJob.jobName()
    implicit val responseExhaustEncoder = Encoders.product[UserInfoExhaustReport]
    val batch1Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/$filePath.csv")
      .as[UserInfoExhaustReport]
      .collectAsList()
      .asScala

    batch1Results.size should be (4)
    batch1Results.map {res => res.`Collection Id`}.toList should contain atLeastOneElementOf List("do_1130928636168192001667")
    batch1Results.map {res => res.`Collection Name`}.toList should contain atLeastOneElementOf List("24 aug course")
    batch1Results.map {res => res.`Batch Name`}.toList should contain atLeastOneElementOf List("Basic Java")
    batch1Results.map {res => res.`Batch Id`}.toList should contain atLeastOneElementOf List("BatchId_batch-001")
    batch1Results.map {res => res.`User UUID`}.toList should contain theSameElementsAs List("user-001", "user-002", "user-003", "user-004")
    batch1Results.map {res => res.`State`}.toList should contain theSameElementsAs List("Karnataka", "Karnataka", "Andhra Pradesh", "Delhi")
    batch1Results.map {res => res.`District`}.toList should contain theSameElementsAs List("bengaluru", "bengaluru", "bengaluru", "babarpur")
    batch1Results.map {res => res.`Org Name`}.toList should contain atLeastOneElementOf List("Pre-prod Custodian Organization")

    val pResponse = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='userinfo-exhaust'")

    while(pResponse.next()) {
      pResponse.getString("status") should be ("SUCCESS")
      pResponse.getString("err_message") should be ("")
      pResponse.getString("dt_job_submitted") should be ("2020-10-19 05:58:18.666")
      pResponse.getString("download_urls") should be (s"{reports/userinfo-exhaust/batch-001_userinfo_${getDate()}.zip}")
      pResponse.getString("dt_file_created") should be (null)
      pResponse.getString("iteration") should be ("0")
    }

    new HadoopFileUtil().delete(spark.sparkContext.hadoopConfiguration, outputLocation)

    UserInfoExhaustJob.canZipExceptionBeIgnored() should be (false)
  }

  it should "generate the user info report with all the users for a batch with requested_channel as System" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1131350140968632321230_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F', 'userinfo-exhaust', 'SUBMITTED', '{\"batchId\": \"batch-001\"}', 'user-002', '0130107621805015045', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.UserInfoExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"localhost","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"12","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"UserInfo Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    UserInfoExhaustJob.execute()

  }

  it should "insert status as FAILED as encryption key not provided" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration) VALUES ('do_1131350140968632321230_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F', 'userinfo-exhaust', 'SUBMITTED', '{\"batchId\": \"batch-001\"}', 'user-002', 'b00bc992ef25f1a9a8d63291e20efc8d', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0);")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.UserInfoExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"localhost","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"12","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"UserInfo Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    UserInfoExhaustJob.execute()

    val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='userinfo-exhaust'")
    while (postgresQuery.next()) {
      postgresQuery.getString("status") should be ("FAILED")
      postgresQuery.getString("err_message") should be ("Invalid request")
      postgresQuery.getString("download_urls") should be ("{}")
    }
  }

  it should "insert status as FAILED as request_data not present" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1131350140968632321230_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F', 'userinfo-exhaust', 'SUBMITTED', '{\"batchId\": \"\", \"searchFilter\": {}}', 'user-002', 'channel-01', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test123');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.UserInfoExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"localhost","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"12","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"UserInfo Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    UserInfoExhaustJob.execute()

    val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='userinfo-exhaust'")
    while (postgresQuery.next()) {
      postgresQuery.getString("status") should be ("FAILED")
      postgresQuery.getString("err_message") should be ("No data found")
      postgresQuery.getString("download_urls") should be ("{}")
    }

  }

  it should "fail as batchId is present in onDemand mode" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1131350140968632321230_batch-002:channel-01', '37564CF8F134EE7532F125651B51D17F', 'userinfo-exhaust', 'SUBMITTED', '{}', 'user-002', 'channel-01', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.UserInfoExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"localhost","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"12","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"UserInfo Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    UserInfoExhaustJob.execute()
    val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='userinfo-exhaust'")
    while (postgresQuery.next()) {
      postgresQuery.getString("status") should be ("FAILED")
      postgresQuery.getString("err_message") should be ("Invalid request")
      postgresQuery.getString("download_urls") should be ("{}")
      postgresQuery.getString("iteration") should be ("1")
    }

  }

  it should "fail as userConsent is not present" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1130505638695649281726_batch-002:channel-01', '37564CF8F134EE7532F125651B51D17F', 'userinfo-exhaust', 'SUBMITTED', '{\"batchId\": \"batch-002\"}', 'user-002', 'channel-01', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.UserInfoExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"localhost","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"12","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"UserInfo Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    UserInfoExhaustJob.execute()
    val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='userinfo-exhaust'")
    while (postgresQuery.next()) {
      postgresQuery.getString("status") should be ("FAILED")
      postgresQuery.getString("err_message") should be ("""Invalid request. User info exhaust is not applicable for collections which don't request for user consent to share data""")
      postgresQuery.getString("download_urls") should be ("{}")
      postgresQuery.getString("iteration") should be ("1")
    }

  }

  it should "should run with onDemand mode as modelParams is not present" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1131350140968632321230_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F', 'progress-exhaust', 'SUBMITTED', '{\"batchId\": \"batch-001\"}', 'user-002', 'channel-01', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.ProgressExhaustJob","parallelization":8,"appName":"Progress Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig
    UserInfoExhaustJob.execute()
  }

  it should "should run the job in standAlone mode" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1131350140968632321230_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F', 'userinfo-exhaust', 'SUBMITTED', '{\"batchId\": \"batch-001\"}', 'user-002', 'channel-01', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.ProgressExhaustJob","modelParams":{"store":"local","mode":"standalone","batchFilter":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"localhost","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"12","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":"", "batchId": "batch-001"},"parallelization":8,"appName":"Progress Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    UserInfoExhaustJob.execute()
  }

  it should "execute the job successfully with searchFilters" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1131350140968632321230_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F', 'userinfo-exhaust', 'SUBMITTED', '{\"batchId\": \"batch-001\"}', 'user-002', 'channel-01', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.ProgressExhaustJob","modelParams":{"store":"local","mode":"standalone","searchFilter":{"request":{"filters":{"framework":"TPD"},"sort_by":{"createdOn":"desc"},"limit":10000,"fields":["framework","identifier","name","channel","userConsent"]}},"sparkElasticsearchConnectionHost":"localhost","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"12","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"Progress Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    UserInfoExhaustJob.execute()
  }

  //Unit test case for save and update requests in OnDemandExhaustJob
  it should "execute the update and save request method" in {
    implicit val fc = new FrameworkContext()
    val jobRequest = JobRequest("'do_1131350140968632321230_batch-001:channel-01'", "123", "userinfo-exhaust", "SUBMITTED", """{\"batchId\": \"batch-001\"}""", "user-002", "channel-01", System.currentTimeMillis(), None, None, None, None, Option(""), Option(0), Option("test-123"))
    val jobRequestArr = Array(jobRequest)
    val outputLocation = AppConf.getConfig("collection.exhaust.store.prefix")
    val storageConfig = StorageConfig("local", "", outputLocation)
    implicit val conf = spark.sparkContext.hadoopConfiguration

    UserInfoExhaustJob.saveRequests(storageConfig, jobRequestArr)
  }

  //Unit Testcases for test coverage
  it should "execute the methods and return final results" in {
    //Testcase1: getUserEnrolmentDF- persist false
    val userEnrolDF = UserInfoExhaustJob.getUserEnrolmentDF(false)
    userEnrolDF.storageLevel.useMemory should be (false)

    //Testcase2: getCollectionBatchDF- persist: true
    val batch = UserInfoExhaustJob.getCollectionBatchDF(true)
    batch.storageLevel.useMemory should be (true)

    //TestCase3: getUserCacheDF- persist: true
    val userDF = UserInfoExhaustJob.getUserCacheDF(UserInfoExhaustJob.getUserCacheColumns(),true)
    userDF.storageLevel.useMemory should be (true)

    //Testcase4: UDFUtils
    val userMap = UDFUtils.fromJSONFun("""{"declared-email": "test@gmail.com"}""")
    userMap.map(f => f._1).toList should contain theSameElementsAs List("declared-email")
    userMap.map(f => f._2).toList should contain theSameElementsAs List("test@gmail.com")
  }

  def getDate(): String = {
    val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
    dateFormat.print(System.currentTimeMillis());
  }
}