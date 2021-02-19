package org.sunbird.analytics.exhaust

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{Encoders, SQLContext, SparkSession}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{HadoopFileUtil, JSONUtils}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.exhaust.collection.{AssessmentData, CollectionBatch, CourseData, ProgressExhaustJob}
import org.sunbird.analytics.job.report.BaseReportSpec
import org.sunbird.analytics.util.{EmbeddedCassandra, EmbeddedPostgresql, RedisCacheUtil}
import redis.clients.jedis.Jedis
import redis.embedded.RedisServer

import scala.collection.JavaConverters._

case class ProgressExhaustReport(`Collection Id`: String, `Collection Name`: String, `Batch Id`: String, `Batch Name`: String, `User UUID`: String, `State`: String, `District`: String, `Org Name`: String,
                                 `School Id`: String, `School Name`: String, `Block Name`: String, `Declared Board`: String, `Enrolment Date`: String, `Completion Date`: String, `Certificate Status`: String, `Progress`: String,
                                 `Total Score`: String)
case class ContentHierarchy(identifier: String, hierarchy: String)

class TestProgressExhaustJob extends BaseReportSpec with MockFactory with BaseReportsJob {

  val jobRequestTable = "job_request"
  implicit var spark: SparkSession = _
  var redisServer: RedisServer = _
  var jedis: Jedis = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession();

    redisServer = new RedisServer(6379)
    // redis setup
    if(!redisServer.isActive) {
      redisServer.start();
    }
    val redisConnect = new RedisCacheUtil()
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
    EmbeddedCassandra.close()
    EmbeddedPostgresql.close()
  }

  def setupRedisData(jedis: Jedis): Unit = {
    jedis.hmset("user:user-001", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Manju", "userid": "user-001", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "01250894314817126443", "email": "manju@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-002", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Mahesh", "userid": "user-002", "state": "Andhra Pradesh", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "mahesh@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-003", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Sowmya", "userid": "user-003", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "sowmya@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-004", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Utkarsha", "userid": "user-004", "state": "Delhi", "district": "babarpur", "userchannel": "sunbird-dev", "rootorgid": "01250894314817126443", "email": "utkarsha@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-005", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Isha", "userid": "user-005", "state": "MP", "district": "Jhansi", "userchannel": "sunbird-dev", "rootorgid": "01250894314817126443", "email": "isha@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-006", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Revathi", "userid": "user-006", "state": "Andhra Pradesh", "district": "babarpur", "userchannel": "sunbird-dev", "rootorgid": "01250894314817126443", "email": "revathi@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-007", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Sunil", "userid": "user-007", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "0126391644091351040", "email": "sunil@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-008", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Anoop", "userid": "user-008", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "anoop@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-009", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Kartheek", "userid": "user-009", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "01285019302823526477", "email": "kartheekp@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-010", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Anand", "userid": "user-010", "state": "Tamil Nadu", "district": "Chennai", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "anandp@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-011", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Santhosh", "userid": "user-010", "state": "Tamil Nadu", "district": "Chennai", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "anandp@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.hmset("user:user-012", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Rayulu", "userid": "user-010", "state": "Tamil Nadu", "district": "Chennai", "userchannel": "sunbird-dev", "rootorgid": "0130107621805015045", "email": "anandp@ilimi.in", "usersignintype": "Validated"};"""))

    jedis.close()
  }

  "ProgressExhaustReport" should "generate the report with all the correct data" in {

    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1130928636168192001667_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F', 'progress-exhaust', 'SUBMITTED', '{\"batchId\": \"batch-001\"}', 'user-002', 'b00bc992ef25f1a9a8d63291e20efc8d', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.ProgressExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"{{ sunbird_es_host }}","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"12","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"Progress Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    ProgressExhaustJob.execute()

    val outputLocation = AppConf.getConfig("collection.exhaust.store.prefix")
    val outputDir = "progress-exhaust"
    val batch1 = "batch-001"
    val filePath = ProgressExhaustJob.getFilePath(batch1)
    val jobName = ProgressExhaustJob.jobName()

    implicit val responseExhaustEncoder = Encoders.product[ProgressExhaustReport]
    val batch1Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/$filePath.csv").as[ProgressExhaustReport].collectAsList().asScala

    batch1Results.size should be (4)
    batch1Results.map(f => f.`Collection Id`).toList should contain atLeastOneElementOf List("do_1130928636168192001667")
    batch1Results.map(f => f.`Collection Name`).toList should contain atLeastOneElementOf List("24 aug course")
    batch1Results.map(f => f.`Batch Id`).toList should contain atLeastOneElementOf List("BatchId_batch-001")
    batch1Results.map(f => f.`Batch Name`).toList should contain atLeastOneElementOf List("Basic Java")
    batch1Results.map {res => res.`User UUID`}.toList should contain theSameElementsAs List("user-001", "user-002", "user-003", "user-004")
    batch1Results.map {res => res.`State`}.toList should contain theSameElementsAs List("Karnataka", "Andhra Pradesh", "Karnataka", "Delhi")
    batch1Results.map {res => res.`District`}.toList should contain theSameElementsAs List("bengaluru", "bengaluru", "bengaluru", "babarpur")
    batch1Results.map(f => f.`Enrolment Date`).toList should contain allElementsOf  List("15/11/2019")
    batch1Results.map(f => f.`Completion Date`).toList should contain allElementsOf  List(null)
    batch1Results.map(f => f.`Progress`).toList should contain allElementsOf  List("100")

    val pResponse = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='progress-exhaust'")
    val reportDate = getDate("yyyyMMdd").format(Calendar.getInstance().getTime())

    while(pResponse.next()) {
      pResponse.getString("status") should be ("SUCCESS")
      pResponse.getString("err_message") should be ("")
      pResponse.getString("dt_job_submitted") should be ("2020-10-19 05:58:18.666")
      pResponse.getString("download_urls") should be (s"""{reports/progress-exhaust/batch-001_progress_${reportDate}.zip}""")
      pResponse.getString("dt_file_created") should be (null)
      pResponse.getString("iteration") should be ("0")
    }

    new HadoopFileUtil().delete(spark.sparkContext.hadoopConfiguration, outputLocation)

    //Test coverage for filterAssessmentsFromHierarchy method
    val assessmentData = ProgressExhaustJob.filterAssessmentsFromHierarchy(List(), List(), AssessmentData("do_1130928636168192001667", List()))
    assessmentData.courseid should be ("do_1130928636168192001667")
    assert(assessmentData.assessmentIds.isEmpty)

  }

  it should "generate the report having batchFilter" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1130928636168192001667_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F', 'progress-exhaust', 'SUBMITTED', '{\"batchFilter\": [\"batch-001\", \"batch-002\"]}', 'user-002', '0130107621805015045', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.ProgressExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"{{ sunbird_es_host }}","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"12","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"Progress Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    ProgressExhaustJob.execute()

    val outputLocation = AppConf.getConfig("collection.exhaust.store.prefix")
    val outputDir = "progress-exhaust"
    val batch = "batch-002"
    var filePath = ProgressExhaustJob.getFilePath(batch)
    val jobName = ProgressExhaustJob.jobName()

    implicit val responseExhaustEncoder = Encoders.product[ProgressExhaustReport]
    var batch1Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/$filePath.csv").as[ProgressExhaustReport].collectAsList().asScala

    batch1Results.size should be (2)
    batch1Results.map(f => f.`Collection Id`).toList should contain atLeastOneElementOf List("do_1130505638695649281726")
    batch1Results.map(f => f.`Collection Name`).toList should contain atLeastOneElementOf List("Feel Good Book-3.1----Copied")
    batch1Results.map(f => f.`Batch Id`).toList should contain atLeastOneElementOf List("BatchId_batch-002")
    batch1Results.map(f => f.`Batch Name`).toList should contain atLeastOneElementOf List("Basic C++")
    batch1Results.map {res => res.`User UUID`}.toList should contain theSameElementsAs List("user-011", "user-012")
    batch1Results.map {res => res.`State`}.toList should contain allElementsOf List("Tamil Nadu")
    batch1Results.map {res => res.`District`}.toList should contain allElementsOf List("Chennai")
    batch1Results.map(f => f.`Enrolment Date`).toList should contain allElementsOf  List("15/11/2019")
    batch1Results.map(f => f.`Completion Date`).toList should contain allElementsOf  List(null)
    batch1Results.map(f => f.`Progress`).toList should contain allElementsOf  List("100")


    val batch1 = "batch-001"
    filePath = ProgressExhaustJob.getFilePath(batch1)
    batch1Results = spark.read.format("csv").option("header", "true")
      .load(s"$outputLocation/$filePath.csv").as[ProgressExhaustReport].collectAsList().asScala

    batch1Results.size should be (2)
    batch1Results.map(f => f.`Collection Id`).toList should contain atLeastOneElementOf List("do_1130928636168192001667")
    batch1Results.map(f => f.`Collection Name`).toList should contain atLeastOneElementOf List("24 aug course")
    batch1Results.map(f => f.`Batch Id`).toList should contain atLeastOneElementOf List("BatchId_batch-001")
    batch1Results.map(f => f.`Batch Name`).toList should contain atLeastOneElementOf List("Basic Java")
    batch1Results.map {res => res.`User UUID`}.toList should contain theSameElementsAs List("user-002", "user-003")
    batch1Results.map {res => res.`State`}.toList should contain theSameElementsAs List("Karnataka", "Andhra Pradesh")
    batch1Results.map {res => res.`District`}.toList should contain theSameElementsAs List("bengaluru", "bengaluru")
    batch1Results.map(f => f.`Enrolment Date`).toList should contain allElementsOf  List("15/11/2019")
    batch1Results.map(f => f.`Completion Date`).toList should contain allElementsOf  List(null)
    batch1Results.map(f => f.`Progress`).toList should contain allElementsOf  List("100")

    val pResponse = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='progress-exhaust'")
    val reportDate = getDate("yyyyMMdd").format(Calendar.getInstance().getTime())

    while(pResponse.next()) {
      pResponse.getString("status") should be ("SUCCESS")
      pResponse.getString("err_message") should be ("")
      pResponse.getString("dt_job_submitted") should be ("2020-10-19 05:58:18.666")
      pResponse.getString("download_urls") should be (s"""{reports/progress-exhaust/batch-002_progress_20210218.zip,reports/progress-exhaust/batch-001_progress_20210218.zip}""")
      pResponse.getString("dt_file_created") should be (null)
      pResponse.getString("iteration") should be ("0")
    }

    new HadoopFileUtil().delete(spark.sparkContext.hadoopConfiguration, outputLocation)
  }

  it should "not generate report if batchid/batchFilter/searchFilter is available" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1130928636168192001667_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F', 'progress-exhaust', 'SUBMITTED', '{}', 'user-002', '0130107621805015045', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.ProgressExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"{{ sunbird_es_host }}","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"12","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"Progress Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    ProgressExhaustJob.execute()

    val pResponse = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='progress-exhaust'")
    val reportDate = getDate("yyyyMMdd").format(Calendar.getInstance().getTime())

    while(pResponse.next()) {
      pResponse.getString("status") should be ("FAILED")
      pResponse.getString("err_message") should be ("Invalid request")
      pResponse.getString("dt_job_submitted") should be ("2020-10-19 05:58:18.666")
      pResponse.getString("download_urls") should be (s"""{}""")
      pResponse.getString("dt_file_created") should be (null)
      pResponse.getString("iteration") should be ("1")
    }
  }

  it should "provide hierarchy data on module level" in {
    implicit val sqlContext: SQLContext = spark.sqlContext
    import sqlContext.implicits._

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.ProgressExhaustJob","modelParams":{"store":"local","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"{{ sunbird_es_host }}","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"12","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"Progress Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    var collectionBatch = CollectionBatch("batch-001","do_1130928636168192001667","Basic Java","0130107621805015045","channel-01","channel-01","Test_TextBook_name_5197942513",Some("Yes"))
    var hierarchyData = List(ContentHierarchy("do_1130928636168192001667","""{"identifier":"do_1130928636168192001667","mimeType":"application/vnd.ekstep.content-collection","visibility":"Default","contentType":"Course","children":[{"parent":"do_112876961957437440179","identifier":"do_1130928636168192001667","lastStatusChangedOn":"2019-09-19T18:15:56.490+0000","code":"2cb4d698-dc19-4f0c-9990-96f49daff753","visibility":"Default","description":"Test_TextBookUnit_desc_8305852636","index":1,"mimeType":"application/vnd.ekstep.content-collection","createdOn":"2019-09-19T18:15:56.489+0000","versionKey":"1568916956489","depth":1,"name":"content_1","lastUpdatedOn":"2019-09-19T18:15:56.490+0000","contentType":"Course","children":[],"status":"Draft"}]}""")).toDF()

    var hierarchyModuleData = ProgressExhaustJob.getCollectionAggWithModuleData(collectionBatch, hierarchyData).collectAsList().asScala
    hierarchyModuleData.map(f => f.getString(0)) should contain theSameElementsAs  List("user-001", "user-002", "user-003", "user-004")
    hierarchyModuleData.map(f => f.getString(1)) should contain allElementsOf List("do_1130928636168192001667")
    hierarchyModuleData.map(f => f.getString(2)) should contain allElementsOf List("batch-001")
    hierarchyModuleData.map(f => f.getInt(3)) should contain allElementsOf List(100)
    hierarchyModuleData.map(f => f.getString(4)) should contain allElementsOf List("do_1130928636168192001667")
    hierarchyModuleData.map(f => f.getInt(5)) should contain allElementsOf List(100)

    // No mimetype, visibility etc available in hierarchy
    hierarchyData = List(ContentHierarchy("do_1130928636168192001667","""{"children":[{"parent":"do_112876961957437440179","identifier":"do_1130928636168192001667","lastStatusChangedOn":"2019-09-19T18:15:56.490+0000","code":"2cb4d698-dc19-4f0c-9990-96f49daff753","visibility":"Default","description":"Test_TextBookUnit_desc_8305852636","index":1,"mimeType":"application/vnd.ekstep.content-collection","createdOn":"2019-09-19T18:15:56.489+0000","versionKey":"1568916956489","depth":1,"name":"content_1","lastUpdatedOn":"2019-09-19T18:15:56.490+0000","contentType":"Course","status":"Draft"}]}""")).toDF()
    val hierarchyModuleData1 = ProgressExhaustJob.getCollectionAggWithModuleData(collectionBatch, hierarchyData).collectAsList().asScala

    hierarchyModuleData1.map(f => f.getString(0)) should contain theSameElementsAs  List("user-001", "user-002", "user-003", "user-004")
    hierarchyModuleData1.map(f => f.getString(1)) should contain allElementsOf List("do_1130928636168192001667")
    hierarchyModuleData1.map(f => f.getString(2)) should contain allElementsOf List("batch-001")
    hierarchyModuleData1.map(f => f.getInt(3)) should contain allElementsOf List(100)
    hierarchyModuleData1.map(f => f.getString(4)) should contain allElementsOf List(null)

    //levelCount is less than depthLevel
    var data =  List(Map("children" -> List(Map("lastStatusChangedOn" -> "2019-09-19T18:15:56.490+0000","parent" -> "do_112876961957437440179","children" -> List(),"name" -> "content_1","createdOn" -> "2019-09-19T18:15:56.489+0000"," lastUpdatedOn" -> "2019-09-19T18:15:56.490+0000", "identifier" -> "do_1130928636168192001667","description" -> "Test_TextBookUnit_desc_8305852636","versionKey" -> "1568916956489","mimeType" -> "application/vnd.ekstep.content-collection","code" -> "2cb4d698-dc19-4f0c-9990-96f49daff753"," contentType" -> "Course"," status" -> "Draft"," depth" -> "1"," visibility" -> "Default"," index" -> "1))"," identifier" -> "do_1130928636168192001667"," mimeType" -> "application/vnd.ekstep.content-collection"," contentType" -> "Course"," visibility" -> "Default"))))
    var prevData = CourseData("do_1130928636168192001667","0",List())
    var parseHierarchyData = ProgressExhaustJob.logTime(ProgressExhaustJob.parseCourseHierarchy(data, 3, prevData, 2), "Execution of ParseCourseHierarchy method")
    assert(parseHierarchyData.equals(prevData))

    //unit testcase for identifier and children not available
    collectionBatch = CollectionBatch("batch-001","do_1130928636168192001667","Basic Java","0130107621805015045","channel-01","channel-01","Test_TextBook_name_5197942513",Some("Yes"))
    hierarchyData = List(ContentHierarchy("do_1130928636168192001667","""{"mimeType":"application/vnd.ekstep.content-collection","visibility":"Default","contentType":"Course"}""")).toDF()

    hierarchyModuleData = ProgressExhaustJob.getCollectionAggWithModuleData(collectionBatch, hierarchyData).collectAsList().asScala
    hierarchyModuleData.map(f => f.getString(0)) should contain theSameElementsAs  List("user-001", "user-002", "user-003", "user-004")
    hierarchyModuleData.map(f => f.getString(1)) should contain allElementsOf List("do_1130928636168192001667")
    hierarchyModuleData.map(f => f.getString(2)) should contain allElementsOf List("batch-001")
    hierarchyModuleData.map(f => f.getInt(3)) should contain allElementsOf List(100)
    hierarchyModuleData.map(f => f.getString(4)) should contain allElementsOf List(null)
  }

  def getDate(pattern: String): SimpleDateFormat = {
    new SimpleDateFormat(pattern)
  }
}
