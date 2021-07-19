package org.sunbird.analytics.uci

import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.ekstep.analytics.framework.util.JSONUtils
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.exhaust.BaseReportsJob
import org.sunbird.analytics.job.report.BaseReportSpec
import org.sunbird.analytics.util.EmbeddedPostgresql

class TestUCIPrivateExhaust extends BaseReportSpec with MockFactory with BaseReportsJob {

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

  "UCI Private Exhaust Report" should "generate the report with all the correct data" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('do_1130928636168192001667_batch-001:channel-01', '37564CF8F134EE7532F125651B51D17F', 'progress-exhaust', 'SUBMITTED', '{\"batchId\": \"batch-001\"}', 'user-002', 'b00bc992ef25f1a9a8d63291e20efc8d', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0, 'test12');")

    EmbeddedPostgresql.execute("INSERT INTO bot (id, name, startingMessage, users, logicIDs, owners, created_at, updated_at, status, description, startDate, endDate, purpose ) VALUES ('fabc64a7-c9b0-4d0b-b8a6-8778757b2bb5', 'Diksha Bot', 'Hello World!', NULL, '{}', ARRAY['channel-001', 'channel-002'], timestamp '2015-01-11 00:51:14', timestamp '2015-01-11 00:51:14', NULL, NULL, NULL, NULL, NULL);")
    EmbeddedPostgresql.execute("INSERT INTO bot (id, name, startingMessage, users, logicIDs, owners, created_at, updated_at, status, description, startDate, endDate, purpose ) VALUES ('56b31f3d-cc0f-49a1-b559-f7709200aa85', 'Sunbird Bot', 'Hello Sunbird!', NULL, '{}','{}', timestamp '2015-01-11 00:51:14', timestamp '2015-01-11 00:51:14', NULL, NULL, NULL, NULL, NULL);")
    EmbeddedPostgresql.execute("INSERT INTO bot (id, name, startingMessage, users, logicIDs, owners, created_at, updated_at, status, description, startDate, endDate, purpose ) VALUES ('5db54579-04bb-4fb7-a9ee-0f9994cfaada', 'COVID', 'What is COVID', NULL, '{}','{}', timestamp '2015-01-11 00:51:14', timestamp '2015-01-11 00:51:14', NULL, NULL, NULL, NULL, NULL);")
    EmbeddedPostgresql.execute("INSERT INTO bot (id, name, startingMessage, users, logicIDs, owners, created_at, updated_at, status, description, startDate, endDate, purpose ) VALUES ('20b0cdec-a9a6-4bd4-8b36-150d45499946', 'SUNDAY FUN', 'How was sunday', NULL, '{}','{}', timestamp '2015-01-11 00:51:14', timestamp '2015-01-11 00:51:14', NULL, NULL, NULL, NULL, NULL);")


    EmbeddedPostgresql.execute("INSERT INTO user_registrations (id, applications_id) VALUES ('4c5abf1b-50d9-4b23-ac9c-1a1489812065', 'fabc64a7-c9b0-4d0b-b8a6-8778757b2bb5');")
    EmbeddedPostgresql.execute("INSERT INTO user_registrations (id, applications_id) VALUES ('4711abba-d06f-49fb-8c63-c80d0d3df790', '56b31f3d-cc0f-49a1-b559-f7709200aa85');")
    EmbeddedPostgresql.execute("INSERT INTO user_registrations (id, applications_id) VALUES ('dda0e8a2-0777-4edd-bb36-d1d8970bafa2', '5db54579-04bb-4fb7-a9ee-0f9994cfaada');")
    EmbeddedPostgresql.execute("INSERT INTO user_registrations (id, applications_id) VALUES ('34ed1146-4eb3-4d3f-8993-b83f62a9aef5', '20b0cdec-a9a6-4bd4-8b36-150d45499946');")


    EmbeddedPostgresql.execute("INSERT INTO users (id, data) VALUES ('4c5abf1b-50d9-4b23-ac9c-1a1489812065', '{\"device\":{\"id\":\"user-001\",\"type\":\"phone\"}}');")
    EmbeddedPostgresql.execute("INSERT INTO users (id, data) VALUES ('4711abba-d06f-49fb-8c63-c80d0d3df790', '{\"device\":{\"id\":\"user-001\",\"type\":\"phone\",\"consent\":true}}');")
    EmbeddedPostgresql.execute("INSERT INTO users (id, data) VALUES ('dda0e8a2-0777-4edd-bb36-d1d8970bafa2', '{\"device\":{\"id\":\"user-001\",\"type\":\"phone\",\"consent\":true}}');")
    EmbeddedPostgresql.execute("INSERT INTO users (id, data) VALUES ('871368b7-a0ed-45e2-92f5-4219fb6789f7', '{\"device\":{\"id\":\"user-001\",\"type\":\"phone\",\"consent\":true}}');")


    EmbeddedPostgresql.execute("INSERT INTO identities (id, users_id, username) VALUES ('4c5abf1b-50d9-4b23-ac9c-1a1489812065', '4c5abf1b-50d9-4b23-ac9c-1a1489812065','X#o89nlfhskl#87923');")
    EmbeddedPostgresql.execute("INSERT INTO identities (id, users_id, username) VALUES ('4711abba-d06f-49fb-8c63-c80d0d3df790', '4711abba-d06f-49fb-8c63-c80d0d3df790','R97ydichfifoshffkkff');")
    EmbeddedPostgresql.execute("INSERT INTO identities (id, users_id, username) VALUES ('dda0e8a2-0777-4edd-bb36-d1d8970bafa2', 'dda0e8a2-0777-4edd-bb36-d1d8970bafa2', 'R97ydichfifoshffkkff');")
    EmbeddedPostgresql.execute("INSERT INTO identities (id, users_id, username) VALUES ('871368b7-a0ed-45e2-92f5-4219fb6789f7', '871368b7-a0ed-45e2-92f5-4219fb6789f7','R97ydichfifoshffkkff');")


    implicit val fc = new FrameworkContext()
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.uci.UCIPrivateExhaust","modelParams":{"store":"local","mode":"OnDemand","sparkElasticsearchConnectionHost":"{{ sunbird_es_host }}","sparkRedisConnectionHost":"localhost","sparkUserDbRedisPort":6341,"sparkUserDbRedisIndex":"0","sparkCassandraConnectionHost":"localhost","fromDate":"","toDate":"","storageContainer":""},"parallelization":8,"appName":"UCI Private Exhaust"}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig
    val proccessedRequest = UCIPrivateExhaust.processRequest("fabc64a7-c9b0-4d0b-b8a6-8778757b2bb5", "channel-005")
    proccessedRequest.show(false)

  }


}
