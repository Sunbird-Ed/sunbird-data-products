package org.sunbird.analytics.exhaust

import org.apache.spark.sql.{Encoders, SparkSession}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{HadoopFileUtil, JSONUtils}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig, StorageConfig}
import org.joda.time.DateTimeZone
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.exhaust.collection.UserInfoExhaustJob
import org.sunbird.analytics.job.report.BaseReportSpec
import org.sunbird.analytics.util.{EmbeddedCassandra, EmbeddedPostgresql, RedisConnect}
import redis.clients.jedis.Jedis
import redis.embedded.RedisServer

import scala.collection.JavaConverters._


case class UserInfoExhaustReport(`Collection Id`: String, `Collection Name`: String, `Batch Id`: String, `Batch Name`: String, `User UUID`: String, `User Name`: String,
                                 `User Type`: String,`User Sub Type`: String, `State`: String, `District`: String, `Block`: String, `Cluster`: String,
                                 `School Id`: String, `School Name`: String, `Org Name`: String, `Email ID`: String, `Mobile Number`: String, `Consent Provided`: String, `Consent Provided Date`: String)

class TestUserInfoExhaustJob extends BaseReportSpec with MockFactory with BaseReportsJob {

  val jobRequestTable = "job_request"
  implicit var spark: SparkSession = _
  var redisServer: RedisServer = _
  var jedis: Jedis = _
  val outputLocation = AppConf.getConfig("collection.exhaust.store.prefix")
  val batchLimit: Int = AppConf.getConfig("data_exhaust.batch.limit.per.request").toInt

  override def beforeAll(): Unit = {
    spark = getSparkSession();
    super.beforeAll()
    redisServer = new RedisServer(6341)
    redisServer.start()
    setupRedisData()
    EmbeddedCassandra.loadData("src/test/resources/exhaust/report_data.cql") // Load test data in embedded cassandra server
    EmbeddedPostgresql.start()
    EmbeddedPostgresql.createJobRequestTable()
  }

  override def afterAll() : Unit = {
    super.afterAll()
    new HadoopFileUtil().delete(spark.sparkContext.hadoopConfiguration, outputLocation)
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
    jedis.hmset("user:user-002", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Mahesh", "userid": "user-002","orgname": "Pre-prod Custodian Organization", "state": "Andhra Pradesh", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "mahesh@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-003", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Sowmya", "userid": "user-003","orgname": "Pre-prod Custodian Organization", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "sowmya@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-004", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Utkarsha", "userid": "user-004","orgname": "Pre-prod Custodian Organization",  "state": "Delhi", "district": "babarpur", "userchannel": "sunbird-dev", "rootorgid": "01250894314817126443", "email": "utkarsha@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-005", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Isha", "userid": "user-005", "state": "MP", "district": "Jhansi", "userchannel": "sunbird-dev", "rootorgid": "01250894314817126443", "email": "isha@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-006", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Revathi", "userid": "user-006", "state": "Andhra Pradesh", "district": "babarpur", "userchannel": "sunbird-dev", "rootorgid": "01250894314817126443", "email": "revathi@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-007", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Sunil", "userid": "user-007", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "0126391644091351040", "email": "sunil@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-008", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Anoop", "userid": "user-008", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "anoop@ilimi.in", "usersignintype": "Validated", "cluster": "Cluster3", "block": "Block3", "usertype": "admin", "usersubtype": "deo", "schooludisecode": "2193754", "schoolname": "Vanasthali PS", "orgname":"Root Org2"};"""))
    jedis.hmset("user:user-009", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Kartheek", "userid": "user-011", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "01285019302823526477", "email": "kartheekp@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-010", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Anand", "userid": "user-012", "state": "Tamil Nadu", "district": "Chennai", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "anandp@ilimi.in", "usersignintype": "Validated", "cluster": "Cluster2", "block": "Block2", "usertype": "admin", "usersubtype": "deo", "schooludisecode": "21937", "schoolnamme": "Vanasthali"};"""))
    jedis.hmset("user:user-015", JSONUtils.deserialize[java.util.Map[String, String]]("""{"cluster":"CLUSTER1","firstname":"Manju","subject":"[\"IRCS\"]","schooludisecode":"3183211","usertype":"administrator","usersignintype":"Validated","language":"[\"English\"]","medium":"[\"English\"]","userid":"user-015","schoolname":"DPS, MATHURA","rootorgid":"01250894314817126443","lastname":"D","framework":"[\"igot_health\"]","orgname":"Root Org2","phone":"","usersubtype":"deo","district":"bengaluru","grade":"[\"Volunteers\"]","block":"BLOCK1","state":"Karnataka","board":"[\"IGOT-Health\"]","email":""};"""))
    jedis.hmset("user:user-016", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Mahesh", "userid": "user-016","orgname": "Pre-prod Custodian Organization", "state": "Andhra Pradesh", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "mahesh@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-017", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Sowmya", "userid": "user-017","orgname": "Pre-prod Custodian Organization", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "sowmya@ilimi.in", "usersignintype": "Validated"};"""))

    jedis.close()
  }

  "UserInfoExhaustJob" should "generate the user info report with all the users for a batch" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1131350140968632321230_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F', 'userinfo-exhaust', 'SUBMITTED', '{\"batchId\": \"batch-001\"}', 'user-002', 'b00bc992ef25f1a9a8d63291e20efc8d', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.UserInfoExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"localhost","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","sparkUserDbRedisPort":6381,"fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"UserInfo Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    UserInfoExhaustJob.execute()

    val outputDir = "response-exhaust"
    val batch1 = "batch-001"
    val requestId = "37564CF8F134EE7532F125651B51D17F"
    val filePath = UserInfoExhaustJob.getFilePath(batch1, requestId)
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
    batch1Results.map {res => res.`Block`}.toList should contain atLeastOneElementOf List("BLOCK1")
    batch1Results.map {res => res.`Cluster`}.toList should contain atLeastOneElementOf List("CLUSTER1")
    batch1Results.map {res => res.`User Type`}.toList should contain atLeastOneElementOf List("administrator")
    batch1Results.map {res => res.`User Sub Type`}.toList should contain atLeastOneElementOf List("deo")

    val user001 = batch1Results.filter(f => f.`User UUID`.equals("user-001"))
    user001.map {f => f.`User UUID`}.head should be ("user-001")
    user001.map {f => f.`State`}.head should be ("Karnataka")
    user001.map {f => f.`District`}.head should be ("bengaluru")
    user001.map {f => f.`Org Name`}.head should be ("Root Org2")
    user001.map {f => f.`Block`}.head should be ("BLOCK1")
    user001.map {f => f.`Cluster`}.head should be ("CLUSTER1")
    user001.map {f => f.`User Type`}.head should be ("administrator")
    user001.map {f => f.`User Sub Type`}.head should be ("deo")
    user001.map {f => f.`School Id`}.head should be ("3183211")
    user001.map {f => f.`School Name`}.head should be ("DPS, MATHURA")
    
    val pResponse = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='userinfo-exhaust'")

    while(pResponse.next()) {
      pResponse.getString("status") should be ("SUCCESS")
      pResponse.getString("err_message") should be ("")
      pResponse.getString("dt_job_submitted") should be ("2020-10-19 05:58:18.666")
      pResponse.getString("download_urls") should be (s"{reports/userinfo-exhaust/$requestId/batch-001_userinfo_${getDate()}.zip}")
      pResponse.getString("dt_file_created") should be (null)
      pResponse.getString("iteration") should be ("0")
    }

    UserInfoExhaustJob.canZipExceptionBeIgnored() should be (false)
  }

  it should "generate the user info report with all the users for a batch with requested_channel as System" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1131350140968632321230_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F', 'userinfo-exhaust', 'SUBMITTED', '{\"batchId\": \"batch-001\"}', 'user-002', '0130107621805015045', '2020-10-19 05:58:18.666', '{}', '2020-10-19 05:58:18.666', 0, '' ,0, 'test12');")

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
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.UserInfoExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"localhost","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","sparkUserDbRedisPort":6381,"fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"UserInfo Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    UserInfoExhaustJob.execute()

    val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='userinfo-exhaust'")
    while (postgresQuery.next()) {
      postgresQuery.getString("status") should be ("FAILED")
      postgresQuery.getString("err_message") should be ("Request should have either of batchId, batchFilter, searchFilter or encrption key")
      postgresQuery.getString("download_urls") should be ("{}")
    }
  }

  it should "insert status as FAILED as request_data not present" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1131350140968632321230_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F', 'userinfo-exhaust', 'SUBMITTED', '{\"batchId\": \"\", \"searchFilter\": {}}', 'user-002', 'channel-01', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test123');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.UserInfoExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"localhost","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","sparkUserDbRedisPort":6381,"fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"UserInfo Exhaust"}"""
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

  it should "insert status as FAILED as batchLimit exceeded" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1131350140968632321230_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F', 'userinfo-exhaust', 'SUBMITTED', '{\"batchFilter\": [\"batch-001\", \"batch-002\",  \"batch-003\",  \"batch-002\", \"batch-006\"]}', 'user-002', 'channel-01', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test123');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.UserInfoExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"localhost","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","sparkUserDbRedisPort":6381,"fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"UserInfo Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    UserInfoExhaustJob.execute()

    val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='userinfo-exhaust'")
    while (postgresQuery.next()) {
      postgresQuery.getString("status") should be ("FAILED")
      postgresQuery.getString("err_message") should be (s"Number of batches in request exceeded. It should be within $batchLimit")
      postgresQuery.getString("download_urls") should be ("{}")
    }

  }

  it should "insert status as FAILED as request_data is empty" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1131350140968632321230_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F', 'userinfo-exhaust', 'SUBMITTED', '{}', 'user-002', 'channel-01', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test123');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.UserInfoExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"localhost","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","sparkUserDbRedisPort":6381,"fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"UserInfo Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    UserInfoExhaustJob.execute()

    val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='userinfo-exhaust'")
    while (postgresQuery.next()) {
      postgresQuery.getString("status") should be ("FAILED")
      postgresQuery.getString("err_message") should be ("Request should have either of batchId, batchFilter, searchFilter or encrption key")
      postgresQuery.getString("download_urls") should be ("{}")
    }
  }

  it should "fail as batchId is not present in onDemand mode" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1131350140968632321230_batch-002:channel-01', '37564CF8F134EE7532F125651B51D17F', 'userinfo-exhaust', 'SUBMITTED', '{}', 'user-002', 'channel-01', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.UserInfoExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"localhost","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","sparkUserDbRedisPort":6381,"fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"UserInfo Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    UserInfoExhaustJob.execute()
    val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='userinfo-exhaust'")
    while (postgresQuery.next()) {
      postgresQuery.getString("status") should be ("FAILED")
      postgresQuery.getString("err_message") should be ("Request should have either of batchId, batchFilter, searchFilter or encrption key")
      postgresQuery.getString("download_urls") should be ("{}")
      postgresQuery.getString("iteration") should be ("1")
    }

  }

  it should "fail as userConsent is not present" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1130505638695649281726_batch-002:channel-01', '37564CF8F134EE7532F125651B51D17F', 'userinfo-exhaust', 'SUBMITTED', '{\"batchId\": \"batch-002\"}', 'user-002', 'channel-01', '2020-10-19 05:58:18.666', '{}', NULL, '2021-03-30 17:50:18.922', 0, '' ,0, 'test12');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.UserInfoExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"localhost","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","sparkUserDbRedisPort":6381,"fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"UserInfo Exhaust"}"""
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
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1131350140968632321230_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F', 'progress-exhaust', 'SUBMITTED', '{\"batchId\": \"batch-001\"}', 'user-002', 'channel-01', '2020-10-19 05:58:18.666', '{}', '2020-10-19 05:58:18.666', NULL, 0, '' ,0, 'test12');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.UserInfoExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"localhost","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","sparkUserDbRedisPort":6381,"fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"UserInfo Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig
    UserInfoExhaustJob.execute()
  }

  it should "should run the job in standAlone mode" in {

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.UserInfoExhaustJob","modelParams":{"store":"local","mode":"standalone", "batchId": "batch-001","batchFilter":["TPD"],"sparkElasticsearchConnectionHost":"localhost","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","sparkUserDbRedisPort":6381,"fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"UserInfo Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    UserInfoExhaustJob.execute()

    val outputDir = "response-exhaust"
    val batch1 = "batch-001"
    val filePath = UserInfoExhaustJob.getFilePath(batch1, "")
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
    batch1Results.map {res => res.`Block`}.toList should contain atLeastOneElementOf List("BLOCK1")
    batch1Results.map {res => res.`Cluster`}.toList should contain atLeastOneElementOf List("CLUSTER1")
    batch1Results.map {res => res.`User Type`}.toList should contain atLeastOneElementOf List("administrator")
    batch1Results.map {res => res.`User Sub Type`}.toList should contain atLeastOneElementOf List("deo")

  }

  it should "execute the job successfully with searchFilters" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1131350140968632321230_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F', 'userinfo-exhaust', 'SUBMITTED', '{\"batchId\": \"batch-001\"}', 'user-002', 'channel-01', '2020-10-19 05:58:18.666', '{}', NULL, 0, '' ,0, 'test12');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.UserInfoExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"localhost","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","sparkUserDbRedisPort":6381,"fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"UserInfo Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    UserInfoExhaustJob.execute()
  }

  //Unit test case for save and update requests
  it should "execute the update and save request method" in {
    implicit val fc = new FrameworkContext()
    val jobRequest = JobRequest("'do_1131350140968632321230_batch-001:channel-01'", "123", "userinfo-exhaust", "SUBMITTED", """{\"batchId\": \"batch-001\"}""", "user-002", "channel-01", System.currentTimeMillis(), None, None, None, None, Option(""), Option(0), Option("test-123"))
    val req = new JobRequest()
    val jobRequestArr = Array(jobRequest)
    val storageConfig = StorageConfig("local", "", outputLocation)
    implicit val conf = spark.sparkContext.hadoopConfiguration

    UserInfoExhaustJob.saveRequests(storageConfig, jobRequestArr)

  }

  it should "generate the report without modelParams present" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1131350140968632321230_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F', 'userinfo-exhaust', 'SUBMITTED', '{\"batchId\": \"batch-001\"}', 'user-002', 'b00bc992ef25f1a9a8d63291e20efc8d', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.UserInfoExhaustJob","parallelization":8,"appName":"UserInfo Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    UserInfoExhaustJob.execute()

    val outputDir = "response-exhaust"
    val batch1 = "batch-001"
    val requestId = "37564CF8F134EE7532F125651B51D17F"
    val filePath = UserInfoExhaustJob.getFilePath(batch1, requestId)
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
    batch1Results.map {res => res.`Block`}.toList should contain atLeastOneElementOf List("BLOCK1")
    batch1Results.map {res => res.`Cluster`}.toList should contain atLeastOneElementOf List("CLUSTER1")
    batch1Results.map {res => res.`User Type`}.toList should contain atLeastOneElementOf List("administrator")
    batch1Results.map {res => res.`User Sub Type`}.toList should contain atLeastOneElementOf List("deo")

    val pResponse = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='userinfo-exhaust'")

    while(pResponse.next()) {
      pResponse.getString("status") should be ("SUCCESS")
      pResponse.getString("err_message") should be ("")
      pResponse.getString("dt_job_submitted") should be ("2020-10-19 05:58:18.666")
      pResponse.getString("download_urls") should be (s"{reports/userinfo-exhaust/$requestId/batch-001_userinfo_${getDate()}.zip}")
      pResponse.getString("dt_file_created") should be (null)
      pResponse.getString("iteration") should be ("0")
    }
  }

  it should "generate the user info report excluding the user whose consent details are not provided" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1131350140968632321230_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F', 'userinfo-exhaust', 'SUBMITTED', '{\"batchId\": \"batch-003\"}', 'user-002', '01309282781705830427', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.UserInfoExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"localhost","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","sparkUserDbRedisPort":6381,"fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"UserInfo Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    UserInfoExhaustJob.execute()

    val outputDir = "response-exhaust"
    val batch1 = "batch-003"
    val requestId = "37564CF8F134EE7532F125651B51D17F"
    val filePath = UserInfoExhaustJob.getFilePath(batch1, requestId)
    val jobName = UserInfoExhaustJob.jobName()
    implicit val responseExhaustEncoder = Encoders.product[UserInfoExhaustReport]
    val batch1Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/$filePath.csv")
      .as[UserInfoExhaustReport]
      .collectAsList()
      .asScala

    //user-010 is not present in consent table hence will be excluded in the report so the size is 2
    batch1Results.size should be (2)
    //assertion for user-008
    val user001 = batch1Results.filter(f => f.`User UUID`.equals("user-008"))
    user001.foreach(f => println("userdetails: " + JSONUtils.serialize(f)))
    user001.map {f => f.`User UUID`}.head should be ("user-008")
    user001.map {f => f.`Collection Id`}.head should be ("do_1131975645014835201326")
    user001.map {f => f.`Batch Id`}.head should be ("BatchId_batch-003")
    user001.map {f => f.`Collection Name`}.head should be ("ADOPT_BOOK_NCERT2")
    user001.map {f => f.`Batch Name`}.head should be ("Basic C++")
    user001.map {f => f.`User Name`}.head should be ("Anoop")
    user001.map {f => f.`State`}.head should be ("Karnataka")
    user001.map {f => f.`District`}.head should be ("bengaluru")
    user001.map {f => f.`Block`}.head should be ("Block3")
    user001.map {f => f.`Cluster`}.head should be ("Cluster3")
    user001.map {f => f.`Email ID`}.head should be ("anoop@ilimi.in")
    user001.map {f => f.`User Type`}.head should be ("admin")
    user001.map {f => f.`User Sub Type`}.head should be ("deo")
    user001.map {f => f.`School Id`}.head should be ("2193754")
    user001.map {f => f.`School Name`}.head should be ("Vanasthali PS")
    user001.map {f => f.`Org Name`}.head should be ("Root Org2")
    user001.map {f => f.`Consent Provided`}.head should be ("true")
    user001.map {f => f.`Consent Provided Date`}.head should be ("14/06/2021")

    //assertion for user-009
    val user009 = batch1Results.filter(f => f.`User UUID`.equals("user-009"))
    user009.foreach(f => println("userdetails: " + JSONUtils.serialize(f)))
    user009.map {f => f.`User UUID`}.head should be ("user-009")
    user009.map {f => f.`Collection Id`}.head should be ("do_1131975645014835201326")
    user009.map {f => f.`Batch Id`}.head should be ("BatchId_batch-003")
    user009.map {f => f.`Collection Name`}.head should be ("ADOPT_BOOK_NCERT2")
    user009.map {f => f.`Batch Name`}.head should be ("Basic C++")
    user009.map {f => f.`User Name`}.head should be ("Kartheek")
    user009.map {f => f.`State`}.head should be ("Karnataka")
    user009.map {f => f.`District`}.head should be ("bengaluru")
    user009.map {f => f.`Email ID`}.head should be ("kartheekp@ilimi.in")
    user009.map {f => f.`Consent Provided`}.head should be ("true")

    val pResponse = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='userinfo-exhaust'")

    while(pResponse.next()) {
      pResponse.getString("status") should be ("SUCCESS")
      pResponse.getString("err_message") should be ("")
      pResponse.getString("dt_job_submitted") should be ("2020-10-19 05:58:18.666")
      pResponse.getString("download_urls") should be (s"{reports/userinfo-exhaust/$requestId/batch-003_userinfo_${getDate()}.zip}")
      pResponse.getString("dt_file_created") should be (null)
      pResponse.getString("iteration") should be ("0")
    }

    UserInfoExhaustJob.canZipExceptionBeIgnored() should be (false)
  }

  /**
    * user-017 will have consentflag=false and hence will be not be included in the report
    */
  it should "generate the user info report excluding the user who have not provided consent" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1131350140968632321230_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F', 'userinfo-exhaust', 'SUBMITTED', '{\"batchId\": \"batch-006\"}', 'user-002', 'b00bc992ef25f1a9a8d63291e20efc8d', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.UserInfoExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"localhost","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","sparkUserDbRedisPort":6381,"fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"UserInfo Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    UserInfoExhaustJob.execute()

    val batch1 = "batch-006"
    val requestId = "37564CF8F134EE7532F125651B51D17F"
    val filePath = UserInfoExhaustJob.getFilePath(batch1, requestId)
    val jobName = UserInfoExhaustJob.jobName()
    implicit val responseExhaustEncoder = Encoders.product[UserInfoExhaustReport]
    val batch1Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/$filePath.csv")
      .as[UserInfoExhaustReport]
      .collectAsList()
      .asScala

    batch1Results.size should be (2)
    batch1Results.map {res => res.`User UUID`}.toList should contain theSameElementsAs List("user-015", "user-016")

    val user001 = batch1Results.filter(f => f.`User UUID`.equals("user-015"))
    user001.map {f => f.`User UUID`}.head should be ("user-015")
    user001.map {f => f.`State`}.head should be ("Karnataka")
    user001.map {f => f.`District`}.head should be ("bengaluru")
    user001.map {f => f.`Org Name`}.head should be ("Root Org2")
    user001.map {f => f.`Block`}.head should be ("BLOCK1")
    user001.map {f => f.`Cluster`}.head should be ("CLUSTER1")
    user001.map {f => f.`User Type`}.head should be ("administrator")
    user001.map {f => f.`User Sub Type`}.head should be ("deo")
    user001.map {f => f.`School Id`}.head should be ("3183211")
    user001.map {f => f.`School Name`}.head should be ("DPS, MATHURA")

    val pResponse = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='userinfo-exhaust'")

    while(pResponse.next()) {
      pResponse.getString("status") should be ("SUCCESS")
      pResponse.getString("err_message") should be ("")
      pResponse.getString("dt_job_submitted") should be ("2020-10-19 05:58:18.666")
      pResponse.getString("download_urls") should be (s"{reports/userinfo-exhaust/$requestId/batch-006_userinfo_${getDate()}.zip}")
      pResponse.getString("dt_file_created") should be (null)
      pResponse.getString("iteration") should be ("0")
    }
  }

  def getDate(): String = {
    val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
    dateFormat.print(System.currentTimeMillis());
  }
}
