package org.sunbird.analytics.exhaust.uci

import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.exhaust.BaseReportsJob
import org.sunbird.analytics.job.report.BaseReportSpec
import org.sunbird.analytics.util.EmbeddedPostgresql

class TestUCIPrivateExhaustJob extends BaseReportSpec with MockFactory with BaseReportsJob {

  implicit var spark: SparkSession = _
  val jobRequestTable = "job_request"

  override def beforeAll(): Unit = {
    spark = getSparkSession()
    super.beforeAll()
    EmbeddedPostgresql.start()
    EmbeddedPostgresql.createJobRequestTable()
    EmbeddedPostgresql.createConversationTable()
    EmbeddedPostgresql.createUserTable()
    EmbeddedPostgresql.createUserRegistrationTable()
    EmbeddedPostgresql.createIdentitiesTable()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    EmbeddedPostgresql.close()
    spark.close()
  }

  def initializePostgresData(): Unit = {
    loadBotData()
    loadUserRegistrationData()
    loadUsersData()
    loadIdentityData()
  }

  "UCI Private Exhaust Report" should "generate the report with all the correct data" in {
    initializePostgresData()
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('fabc64a7-c9b0-4d0b-b8a6-8778757b2bb5:channel-002', '37564CF8F134EE7532F125651B51D17F', 'uci-private-exhaust', 'SUBMITTED', '{\"conversationId\":\"fabc64a7-c9b0-4d0b-b8a6-8778757b2bb5\"}', 'user-002', 'channel-001', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.UCIPrivateExhaustJob","modelParams":{"store":"local","mode":"OnDemand","fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"UCI Private Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig
    UCIPrivateExhaustJob.execute()

    val outputLocation = AppConf.getConfig("uci.exhaust.store.prefix")
    val requestId = "37564CF8F134EE7532F125651B51D17F"
    val conversationId = "fabc64a7-c9b0-4d0b-b8a6-8778757b2bb5"
    val filePath = UCIPrivateExhaustJob.getFilePath(conversationId, requestId)
    val conversationResult = spark.read.format("csv").option("header", "true").load(s"$outputLocation/$filePath.csv")
    conversationResult.show(false)
    conversationResult.count() should be(1)
    conversationResult.select("Conversation ID").rdd.map(r => r(0)).collect.toList.head should be("fabc64a7-c9b0-4d0b-b8a6-8778757b2bb5")
    conversationResult.select("Conversation Name").rdd.map(r => r(0)).collect.toList.head should be("Diksha Bot")
    conversationResult.select("Decrypted Device ID").rdd.map(r => r(0)).collect.toList.head should be("phone:9380991721")
    conversationResult.select(col = "Encrypted Device ID").rdd.map(r => r(0)).collect.toList.head should be("KYvpl3KzZrR57hFOwq6KpTO1fgXBY8erkqo93dj2mHs=")
    conversationResult.select(col = "Device UUID").rdd.map(r => r(0)).collect.toList.head should be("4c5abf1b-50d9-4b23-ac9c-1a1489812065")
    
  }


  "When consent is False" should "Not show the mobile number in the device id column" in {
    initializePostgresData()
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('fabc64a7fsd-c9b0-4d0b-b8a6-8778757b2bb5:channel-001', '537964CF8F134EE7532F125651B51D153', 'uci-private-exhaust', 'SUBMITTED', '{\"conversationId\":\"5db54579-04bb-4fb7-a9ee-0f9994cfaada\"}', 'user-002', 'channel-001', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.UCIPrivateExhaustJob","modelParams":{"store":"local","mode":"OnDemand","fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"UCI Private Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig
    UCIPrivateExhaustJob.execute()

    val outputLocation = AppConf.getConfig("uci.exhaust.store.prefix")
    val requestId = "537964CF8F134EE7532F125651B51D153"
    val conversationId = "5db54579-04bb-4fb7-a9ee-0f9994cfaada"
    val filePath = UCIPrivateExhaustJob.getFilePath(conversationId, requestId)
    val conversationResult = spark.read.format("csv").option("header", "true").load(s"$outputLocation/$filePath.csv")
    conversationResult.count() should be(1)
    conversationResult.select("Conversation ID").rdd.map(r => r(0)).collect.toList.head should be("5db54579-04bb-4fb7-a9ee-0f9994cfaada")
    conversationResult.select("Conversation Name").rdd.map(r => r(0)).collect.toList.head should be("COVID")
    conversationResult.select("Decrypted Device ID").rdd.map(r => r(0)).collect.toList.head should be("dda0e8a2-0777-4edd-bb36-d1d8970bafa2")
    conversationResult.select(col = "Encrypted Device ID").rdd.map(r => r(0)).collect.toList.head should be("8dsi5k7w7LrI/Kc0S5eU5q96LUd8K7H59LH3TBKOi38=")
    conversationResult.select(col = "Device UUID").rdd.map(r => r(0)).collect.toList.head should be("dda0e8a2-0777-4edd-bb36-d1d8970bafa2")
  }

  it should "update request as FAILED if conversation data is not available" in {
    initializePostgresData()
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute(s"TRUNCATE bot")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration) VALUES ('56b31f3d-cc0f-49a1-b559-f7709200aa85:channel-01', '57564CF8F134EE7532F125651B51D17F', 'uci-private-exhaust', 'SUBMITTED', '{\"conversationId\": \"56b31f3d-cc0f-49a1-b559-f7709200aa85\"}', 'user-002', 'channel-001', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0);")
    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.collection.UCIPrivateExhaustJob","modelParams":{"store":"local","mode":"OnDemand","fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"UCI Private Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig
    UCIPrivateExhaustJob.execute()
    val pResponse = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='uci-private-exhaust'")
    while(pResponse.next()) {
      pResponse.getString("err_message") should be ("No data found for conversation in DB")
      pResponse.getString("status") should be ("FAILED")
    }
  }

  def loadUserRegistrationData(): Unit = {
    val tableName = "user_registrations"
    EmbeddedPostgresql.execute(s"TRUNCATE $tableName")
    EmbeddedPostgresql.execute(s"INSERT INTO $tableName (id, users_id, applications_id) VALUES ('4c5abf1b-50d9-4b23-ac9c-1a1489812065','4c5abf1b-50d9-4b23-ac9c-1a1489812065', 'fabc64a7-c9b0-4d0b-b8a6-8778757b2bb5');")
    EmbeddedPostgresql.execute(s"INSERT INTO $tableName (id, users_id, applications_id) VALUES ('4711abba-d06f-49fb-8c63-c80d0d3df790','4711abba-d06f-49fb-8c63-c80d0d3df790', '56b31f3d-cc0f-49a1-b559-f7709200aa85');")
    EmbeddedPostgresql.execute(s"INSERT INTO $tableName (id, users_id, applications_id) VALUES ('dda0e8a2-0777-4edd-bb36-d1d8970bafa2','dda0e8a2-0777-4edd-bb36-d1d8970bafa2', '5db54579-04bb-4fb7-a9ee-0f9994cfaada');")
    EmbeddedPostgresql.execute(s"INSERT INTO $tableName (id, users_id, applications_id) VALUES ('34ed1146-4eb3-4d3f-8993-b83f62a9aef5','34ed1146-4eb3-4d3f-8993-b83f62a9aef5', '20b0cdec-a9a6-4bd4-8b36-150d45499946');")
  }

  def loadBotData(): Unit = {
    val tableName = "bot"
    EmbeddedPostgresql.execute(s"TRUNCATE $tableName")
    EmbeddedPostgresql.execute(s"INSERT INTO $tableName (id, name, startingMessage, users, logicIDs, owners, created_at, updated_at, status, description, startDate, endDate, purpose,ownerOrgID ) VALUES ('fabc64a7-c9b0-4d0b-b8a6-8778757b2bb5', 'Diksha Bot', 'Hello World!', NULL, '{}', ARRAY['channel-001', 'channel-002'], timestamp '2015-01-11 00:51:14', timestamp '2015-01-11 00:51:14', NULL, NULL, NULL, NULL, NULL, 'channel-001');")
    EmbeddedPostgresql.execute(s"INSERT INTO $tableName (id, name, startingMessage, users, logicIDs, owners, created_at, updated_at, status, description, startDate, endDate, purpose,ownerOrgID ) VALUES ('56b31f3d-cc0f-49a1-b559-f7709200aa85', 'Sunbird Bot', 'Hello Sunbird!', NULL, '{}','{}', timestamp '2015-01-11 00:51:14', timestamp '2015-01-11 00:51:14', NULL, NULL, NULL, NULL, NULL, NULL);")
    EmbeddedPostgresql.execute(s"INSERT INTO $tableName (id, name, startingMessage, users, logicIDs, owners, created_at, updated_at, status, description, startDate, endDate, purpose,ownerOrgID ) VALUES ('5db54579-04bb-4fb7-a9ee-0f9994cfaada', 'COVID', 'What is COVID', NULL, '{}',ARRAY['channel-001'], timestamp '2015-01-11 00:51:14', timestamp '2015-01-11 00:51:14', NULL, NULL, NULL, NULL, NULL, 'channel-001');")
    EmbeddedPostgresql.execute(s"INSERT INTO $tableName (id, name, startingMessage, users, logicIDs, owners, created_at, updated_at, status, description, startDate, endDate, purpose,ownerOrgID ) VALUES ('20b0cdec-a9a6-4bd4-8b36-150d45499946', 'SUNDAY FUN', 'How was sunday', NULL, '{}','{}', timestamp '2015-01-11 00:51:14', timestamp '2015-01-11 00:51:14', NULL, NULL, NULL, NULL, NULL, NULL);")
  }

  def loadUsersData(): Unit = {
    val tableName = "users"
    EmbeddedPostgresql.execute(s"TRUNCATE $tableName")
    EmbeddedPostgresql.execute("INSERT INTO users (id, data) VALUES ('4c5abf1b-50d9-4b23-ac9c-1a1489812065', '{\"data\":{\"device\":{\"id\":\"user-001\",\"type\":\"phone\"}}}');")
    EmbeddedPostgresql.execute("INSERT INTO users (id, data) VALUES ('4711abba-d06f-49fb-8c63-c80d0d3df790', '{\"data\":{\"device\":{\"id\":\"user-001\",\"type\":\"phone\",\"consent\":true}}}');")
    EmbeddedPostgresql.execute("INSERT INTO users (id, data) VALUES ('dda0e8a2-0777-4edd-bb36-d1d8970bafa2', '{\"data\":{\"device\":{\"id\":\"user-001\",\"type\":\"phone\",\"consent\":false}}}');")
    EmbeddedPostgresql.execute("INSERT INTO users (id, data) VALUES ('871368b7-a0ed-45e2-92f5-4219fb6789f7', '{\"data\":{\"device\":{\"id\":\"user-001\",\"type\":\"phone\",\"consent\":true}}}');")
  }

  def loadIdentityData(): Unit = {
    val tableName = "identities"
    EmbeddedPostgresql.execute(s"TRUNCATE $tableName")
    EmbeddedPostgresql.execute(s"INSERT INTO $tableName (id, users_id, username) VALUES ('4c5abf1b-50d9-4b23-ac9c-1a1489812065', '4c5abf1b-50d9-4b23-ac9c-1a1489812065','KYvpl3KzZrR57hFOwq6KpTO1fgXBY8erkqo93dj2mHs=');")
    EmbeddedPostgresql.execute(s"INSERT INTO $tableName (id, users_id, username) VALUES ('4711abba-d06f-49fb-8c63-c80d0d3df790', '4711abba-d06f-49fb-8c63-c80d0d3df790','X0VRa758n+J9Dp1vqpss9lnmsuqJDAPsvQ9zXw2zZdg=');")
    EmbeddedPostgresql.execute(s"INSERT INTO $tableName (id, users_id, username) VALUES ('dda0e8a2-0777-4edd-bb36-d1d8970bafa2', 'dda0e8a2-0777-4edd-bb36-d1d8970bafa2', '8dsi5k7w7LrI/Kc0S5eU5q96LUd8K7H59LH3TBKOi38=');")
    EmbeddedPostgresql.execute(s"INSERT INTO $tableName (id, users_id, username) VALUES ('871368b7-a0ed-45e2-92f5-4219fb6789f7', '871368b7-a0ed-45e2-92f5-4219fb6789f7','giArXJC2351b4EGZyxDaNzO1fgXBY8erkqo93dj2mHs=');")
  }
}
