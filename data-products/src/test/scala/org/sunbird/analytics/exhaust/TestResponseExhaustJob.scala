package org.sunbird.analytics.exhaust

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.exhaust.collection.ResponseExhaustJob
import org.sunbird.analytics.job.report.BaseReportSpec
import org.sunbird.analytics.util.{EmbeddedCassandra, EmbeddedPostgresql, RedisCacheUtil}
import redis.clients.jedis.Jedis
import redis.embedded.RedisServer


class TestResponseExhaustJob extends BaseReportSpec with MockFactory with BaseReportsJob {

  val jobRequestTable = "job_request"
  implicit var spark: SparkSession = _
  var userDF: DataFrame = _
  var redisServer: RedisServer = _
  var jedis: Jedis = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession();

    userDF = spark.read.json("src/test/resources/exhaust/user_data.json").cache()
    redisServer = new RedisServer(6379)
    // redis setup
    if(!redisServer.isActive) {
      redisServer.start();
    }
    val redisConnect = new RedisCacheUtil()
    jedis = redisConnect.getConnection(12)

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

  "TestResponseExhaustJob" should "generate final output as csv and zip files" in {
//    println("redis data: " + JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Manju"}"""))
    jedis.hmset("user:user-001", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Manju", "userid": "user-001", "state": "Karnataka", "district": "bengaluru", "userchannel": "ka", "rootorgid": "01250894314817126443"}"""))
    println("jedis info: " + jedis.info())
    val redisData = jedis.hgetAll("user:user-001")
    println("redisData: " + redisData )
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1130505638695649281726_batch-001:01250894314817126443', '37564CF8F134EE7532F125651B51D17F', 'response-exhaust', 'SUBMITTED', '{\"batchId\": \"batch-001\"}', 'user-002', '01250894314817126443', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, '');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.ProgressExhaustJob","modelParams":{"store":"azure","mode":"OnDemand","batchFilters":["TPD"],"searchFilter":{},"sparkElasticsearchConnectionHost":"localhost","sparkRedisConnectionHost":"localhost","sparkUserDbRedisIndex":"12","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":""},"parallelization":8,"appName":"Progress Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    val jobResponse = ResponseExhaustJob.execute()
  }

}
