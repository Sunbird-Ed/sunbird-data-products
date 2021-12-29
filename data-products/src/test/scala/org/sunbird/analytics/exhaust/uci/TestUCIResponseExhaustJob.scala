package org.sunbird.analytics.exhaust.uci

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date}
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.joda.time.DateTimeZone
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.exhaust.BaseReportsJob
import org.sunbird.analytics.job.report.BaseReportSpec
import org.sunbird.analytics.util.EmbeddedPostgresql

class TestUCIResponseExhaustJob  extends BaseReportSpec with MockFactory with BaseReportsJob {

  implicit var spark: SparkSession = _
  val jobRequestTable = "job_request"

  override def beforeAll(): Unit = {
    spark = getSparkSession()
    super.beforeAll()
    EmbeddedPostgresql.start()
    EmbeddedPostgresql.createJobRequestTable()
    EmbeddedPostgresql.createConversationTable()
    EmbeddedPostgresql.createUserTable()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    EmbeddedPostgresql.close()
    spark.close()
  }

  "UCI Response Exhaust Report" should "generate the report with all the correct data with consent false" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute(s"TRUNCATE bot")
    EmbeddedPostgresql.execute(s"TRUNCATE users")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration) VALUES ('fabc64a7-c9b0-4d0b-b8a6-8778757b2bb5:channel-01', '37564CF8F134EE7532F125651B51D17F', 'uci-response-exhaust', 'SUBMITTED', '{\"conversationId\": \"fabc64a7-c9b0-4d0b-b8a6-8778757b2bb5\"}', 'user-002', 'channel-001', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0);")

    EmbeddedPostgresql.execute("INSERT INTO bot (id, name, startingMessage, users, logicIDs, owners, created_at, updated_at, status, description, startDate, endDate, purpose,ownerOrgID ) VALUES ('fabc64a7-c9b0-4d0b-b8a6-8778757b2bb5', 'Diksha Bot', 'Hello World!', NULL, '{}', ARRAY['channel-001', 'channel-002'], timestamp '2015-01-11 00:51:14', timestamp '2015-01-11 00:51:14', NULL, NULL, timestamp '2021-07-10 00:51:14', timestamp '2021-07-11 00:51:14', NULL, 'channel-001');")
    EmbeddedPostgresql.execute("INSERT INTO bot (id, name, startingMessage, users, logicIDs, owners, created_at, updated_at, status, description, startDate, endDate, purpose,ownerOrgID ) VALUES ('56b31f3d-cc0f-49a1-b559-f7709200aa85', 'Sunbird Bot', 'Hello Sunbird!', NULL, '{}','{}', timestamp '2015-01-11 00:51:14', timestamp '2015-01-11 00:51:14', NULL, NULL, timestamp '2021-07-10 00:51:14', timestamp '2021-07-11 00:51:14', NULL, 'channel-001');")

    EmbeddedPostgresql.execute("INSERT INTO users (id, data) VALUES ('4711abba-d06f-49fb-8c63-c80d0d3df790', '{\"device\":{\"id\":\"user-001\",\"type\":\"phone\"}}');")
    EmbeddedPostgresql.execute("INSERT INTO users (id, data) VALUES ('6798fa5a2d8335c43ba64d5b96a944b9', '{\"device\":{\"id\":\"user-001\",\"type\":\"phone\",\"consent\":false}}');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"local","queries":[{"file":"src/test/resources/exhaust/uci/telemetry_data.log"}]},"model":"org.sunbird.analytics.uci.UCIResponseExhaust","modelParams":{"store":"local","botPdataId":"dev.UCI.sunbird","mode":"OnDemand","fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"UCI Response Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    UCIResponseExhaustJob.execute()

    val pResponse = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='uci-response-exhaust'")

    while(pResponse.next()) {
      pResponse.getString("status") should be ("SUCCESS")
      pResponse.getString("download_urls") should not be empty
      pResponse.getString("download_urls") should be (s"{src/test/resources/exhaust-reports/uci-response-exhaust/37564CF8F134EE7532F125651B51D17F/fabc64a7-c9b0-4d0b-b8a6-8778757b2bb5_response_${getDate()}.csv}")
    }
  }

  it should "generate the report with all the correct data with consent true" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute(s"TRUNCATE bot")
    EmbeddedPostgresql.execute(s"TRUNCATE users")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration) VALUES ('fabc64a7-c9b0-4d0b-b8a6-8778757b2bb5:channel-01', '37564CF8F134EE7532F125651B51D17F', 'uci-response-exhaust', 'SUBMITTED', '{\"conversationId\": \"fabc64a7-c9b0-4d0b-b8a6-8778757b2bb5\"}', 'user-002', 'channel-001', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0);")

    EmbeddedPostgresql.execute("INSERT INTO bot (id, name, startingMessage, users, logicIDs, owners, created_at, updated_at, status, description, startDate, endDate, purpose,ownerOrgID ) VALUES ('fabc64a7-c9b0-4d0b-b8a6-8778757b2bb5', 'Diksha Bot', 'Hello World!', NULL, '{}', ARRAY['channel-001', 'channel-002'], timestamp '2015-01-11 00:51:14', timestamp '2015-01-11 00:51:14', NULL, NULL, timestamp '2021-07-10 00:51:14', timestamp '2021-07-11 00:51:14', NULL, 'channel-001');")
    EmbeddedPostgresql.execute("INSERT INTO bot (id, name, startingMessage, users, logicIDs, owners, created_at, updated_at, status, description, startDate, endDate, purpose,ownerOrgID ) VALUES ('56b31f3d-cc0f-49a1-b559-f7709200aa85', 'Sunbird Bot', 'Hello Sunbird!', NULL, '{}','{}', timestamp '2015-01-11 00:51:14', timestamp '2015-01-11 00:51:14', NULL, NULL, timestamp '2021-07-10 00:51:14', timestamp '2021-07-11 00:51:14', NULL, 'channel-001');")

    EmbeddedPostgresql.execute("INSERT INTO users (id, data) VALUES ('4711abba-d06f-49fb-8c63-c80d0d3df790', '{\"device\":{\"id\":\"user-001\",\"type\":\"phone\"}}');")
    EmbeddedPostgresql.execute("INSERT INTO users (id, data) VALUES ('6798fa5a2d8335c43ba64d5b96a944b9', '{\"device\":{\"id\":\"user-001\",\"type\":\"phone\",\"consent\":true}}');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"local","queries":[{"file":"src/test/resources/exhaust/uci/telemetry_data.log"}]},"model":"org.sunbird.analytics.uci.UCIResponseExhaust","modelParams":{"store":"local","botPdataId":"dev.UCI.sunbird","mode":"OnDemand","fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"UCI Response Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    UCIResponseExhaustJob.execute()

    val pResponse = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='uci-response-exhaust'")

    while(pResponse.next()) {
      pResponse.getString("status") should be ("SUCCESS")
      pResponse.getString("download_urls") should not be empty
      pResponse.getString("download_urls") should be (s"{src/test/resources/exhaust-reports/uci-response-exhaust/37564CF8F134EE7532F125651B51D17F/fabc64a7-c9b0-4d0b-b8a6-8778757b2bb5_response_${getDate()}.csv}")
    }
  }

  it should "generate the report with all the correct data with telemetry filter logic" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute(s"TRUNCATE bot")
    EmbeddedPostgresql.execute(s"TRUNCATE users")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration) VALUES ('fabc64a7-c9b0-4d0b-b8a6-8778757b2bb5:channel-01', '37564CF8F134EE7532F125651B51D17F', 'uci-response-exhaust', 'SUBMITTED', '{\"conversationId\": \"fabc64a7-c9b0-4d0b-b8a6-8778757b2bb5\"}', 'user-002', 'channel-001', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0);")

    EmbeddedPostgresql.execute("INSERT INTO bot (id, name, startingMessage, users, logicIDs, owners, created_at, updated_at, status, description, startDate, endDate, purpose,ownerOrgID ) VALUES ('fabc64a7-c9b0-4d0b-b8a6-8778757b2bb5', 'Diksha Bot', 'Hello World!', NULL, '{}', ARRAY['channel-001', 'channel-002'], timestamp '2015-01-11 00:51:14', timestamp '2015-01-11 00:51:14', NULL, NULL, timestamp '2021-07-10 00:51:14', timestamp '2021-07-11 00:51:14', NULL, 'channel-001');")
    EmbeddedPostgresql.execute("INSERT INTO bot (id, name, startingMessage, users, logicIDs, owners, created_at, updated_at, status, description, startDate, endDate, purpose,ownerOrgID ) VALUES ('56b31f3d-cc0f-49a1-b559-f7709200aa85', 'Sunbird Bot', 'Hello Sunbird!', NULL, '{}','{}', timestamp '2015-01-11 00:51:14', timestamp '2015-01-11 00:51:14', NULL, NULL, timestamp '2021-07-10 00:51:14', timestamp '2021-07-11 00:51:14', NULL, 'channel-001');")

    EmbeddedPostgresql.execute("INSERT INTO users (id, data) VALUES ('4711abba-d06f-49fb-8c63-c80d0d3df790', '{\"device\":{\"id\":\"user-001\",\"type\":\"phone\"}}');")
    EmbeddedPostgresql.execute("INSERT INTO users (id, data) VALUES ('6798fa5a2d8335c43ba64d5b96a944b9', '{\"device\":{\"id\":\"user-001\",\"type\":\"phone\",\"consent\":false}}');")

    implicit val fc = new FrameworkContext()

    val strConfig = """{"search":{"type":"local","queries":[{"file":"src/test/resources/exhaust/uci/telemetry_data_2.log"}]}, "filters":[{"name":"eid","operator":"EQ","value":"ASSESS"}], "model":"org.sunbird.analytics.uci.UCIResponseExhaust","modelParams":{"store":"local","botPdataId":"dev.UCI.sunbird","mode":"OnDemand","fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"UCI Response Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    UCIResponseExhaustJob.execute()

    val pResponse = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='uci-response-exhaust'")

    while(pResponse.next()) {
      pResponse.getString("status") should be ("SUCCESS")
      pResponse.getString("download_urls") should not be empty
      pResponse.getString("download_urls") should be (s"{src/test/resources/exhaust-reports/uci-response-exhaust/37564CF8F134EE7532F125651B51D17F/fabc64a7-c9b0-4d0b-b8a6-8778757b2bb5_response_${getDate()}.csv}")
    }
  }

  it should "Able to get the correct start date and end date values from the API" in {
    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"local","queries":[{"file":"src/test/resources/exhaust/uci/telemetry_data.log"}]},"model":"org.sunbird.analytics.uci.UCIResponseExhaust","modelParams":{"store":"local","botPdataId":"dev.UCI.sunbird","mode":"OnDemand","fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"UCI Response Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig
    val request_data = Map("conversationId" -> "56b31f3d-cc0f-49a1-b559-f7709200aa85", "startDate" -> "2022-01-01", "endDate" -> "2022-01-02")
    val spark = SparkSession.builder.getOrCreate
    import spark.implicits._
    val conversationDF = Seq(
      ("2022-01-01", "2022-01-02", "56b31f3d-cc0f-49a1-b559-f7709200aa85"),
      ("2021-01-03", "2021-01-03", "56b31f3d-cc0f-49a1-b559-f7709200aa85")
    ).toDF("startDate", "endDate", "conversationId")

    val conversationDates = UCIResponseExhaustJob.getConversationDates(request_data, conversationDF)
    conversationDates("conversationStartDate") should be ("2022-01-01")
    conversationDates("conversationEndDate") should be ("2022-01-02")
  }

  it should "Able to get the correct start date and end date values from the Conversation Table" in {
    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"local","queries":[{"file":"src/test/resources/exhaust/uci/telemetry_data.log"}]},"model":"org.sunbird.analytics.uci.UCIResponseExhaust","modelParams":{"store":"local","botPdataId":"dev.UCI.sunbird","mode":"OnDemand","fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"UCI Response Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig
    val request_data = Map("conversationId" -> "56b31f3d-cc0f-49a1-b559-f7709200aa85")
    val spark = SparkSession.builder.getOrCreate
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val conversationDF = Seq(
      ("2021-01-02", "2021-01-03", "56b31f3d-cc0f-49a1-b559-f7709200aa85"),
      ("2021-01-03", "2021-01-03", "56b31f3d-cc0f-49a1-b559-f7709200aa85")
    ).toDF("startDate", "endDate", "conversationId").withColumn("startDate", to_date(col("startDate"), "yyyy-MM-dd")).withColumn("endDate", to_date(col("endDate"), "yyyy-MM-dd"))

    val conversationDates = UCIResponseExhaustJob.getConversationDates(request_data, conversationDF)
    conversationDates("conversationStartDate") should be ("2021-01-02")
    conversationDates("conversationEndDate") should be ("2021-01-03")
  }

  it should "Able to get the correct start date and end date values when both API and Tables are not defined" in {
    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"local","queries":[{"file":"src/test/resources/exhaust/uci/telemetry_data.log"}]},"model":"org.sunbird.analytics.uci.UCIResponseExhaust","modelParams":{"store":"local","botPdataId":"dev.UCI.sunbird","mode":"OnDemand","fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"UCI Response Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig
    val request_data = Map("conversationId" -> "56b31f3d-cc0f-49a1-b559-f7709200aa85")
    val spark = SparkSession.builder.getOrCreate
    import spark.implicits._
    val conversationDF = Seq(
      (null, null, "56b31f3d-cc0f-49a1-b559-f7709200aa85"),
      (null, null, "56b31f3d-cc0f-49a1-b559-f7709200aa85")
    ).toDF("startDate", "endDate", "conversationId").withColumn("startDate", to_date(col("startDate"), "yyyy-MM-dd")).withColumn("endDate", to_date(col("endDate"), "yyyy-MM-dd"))
    val conversationDates = UCIResponseExhaustJob.getConversationDates(request_data, conversationDF)
    conversationDates("conversationStartDate") should not be(null)
    conversationDates("conversationEndDate") should not be(null)

  }

  it should "update request as FAILED if conversation data is not available" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute(s"TRUNCATE bot")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration) VALUES ('56b31f3d-cc0f-49a1-b559-f7709200aa85:channel-01', '57564CF8F134EE7532F125651B51D17F', 'uci-response-exhaust', 'SUBMITTED', '{\"conversationId\": \"56b31f3d-cc0f-49a1-b559-f7709200aa85\"}', 'user-002', 'channel-001', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0);")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"local","queries":[{"file":"src/test/resources/exhaust/uci/telemetry_data.log"}]},"model":"org.sunbird.analytics.uci.UCIResponseExhaust","modelParams":{"store":"local","botPdataId":"dev.UCI.sunbird","mode":"OnDemand","fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"UCI Response Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    UCIResponseExhaustJob.execute()

    val pResponse = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='uci-response-exhaust'")

    while(pResponse.next()) {
      pResponse.getString("err_message") should be ("No data found for conversation in DB")
      pResponse.getString("status") should be ("FAILED")
    }
  }

  it should "update request as FAILED if telemetry assess data is not available" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute(s"TRUNCATE bot")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration) VALUES ('56b31f3d-cc0f-49a1-b559-f7709200aa85:channel-01', '57564CF8F134EE7532F125651B51D17F', 'uci-response-exhaust', 'SUBMITTED', '{\"conversationId\": \"56b31f3d-cc0f-49a1-b559-f7709200aa85\"}', 'user-002', 'channel-001', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0);")

    EmbeddedPostgresql.execute("INSERT INTO bot (id, name, startingMessage, users, logicIDs, owners, created_at, updated_at, status, description, startDate, endDate, purpose,ownerOrgID ) VALUES ('fabc64a7-c9b0-4d0b-b8a6-8778757b2bb5', 'Diksha Bot', 'Hello World!', NULL, '{}', ARRAY['channel-001', 'channel-002'], timestamp '2015-01-11 00:51:14', timestamp '2015-01-11 00:51:14', NULL, NULL, timestamp '2021-07-10 00:51:14', timestamp '2021-07-11 00:51:14', NULL, 'channel-001');")
    EmbeddedPostgresql.execute("INSERT INTO bot (id, name, startingMessage, users, logicIDs, owners, created_at, updated_at, status, description, startDate, endDate, purpose,ownerOrgID ) VALUES ('56b31f3d-cc0f-49a1-b559-f7709200aa85', 'Sunbird Bot', 'Hello Sunbird!', NULL, '{}', ARRAY['channel-001', 'channel-002'], timestamp '2015-01-11 00:51:14', timestamp '2015-01-11 00:51:14', NULL, NULL, timestamp '2021-07-10 00:51:14', timestamp '2021-07-11 00:51:14', NULL, 'channel-001');")

    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"local","queries":[{"file":"src/test/resources/exhaust/uci/telemetry_data.log"}]},"model":"org.sunbird.analytics.uci.UCIResponseExhaust","modelParams":{"store":"local","botPdataId":"dev.UCI.sunbird","mode":"OnDemand","fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"UCI Response Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig

    UCIResponseExhaustJob.execute()

    val pResponse = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='uci-response-exhaust'")

    while(pResponse.next()) {
      pResponse.getString("err_message") should be ("No data found")
      pResponse.getString("status") should be ("FAILED")
    }
  }

  def getDate(): String = {
    val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
    dateFormat.print(System.currentTimeMillis());
  }
}

