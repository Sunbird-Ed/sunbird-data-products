package org.sunbird.analytics.exhaust

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig, StorageConfig}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{HadoopFileUtil, JSONUtils}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.job.report.BaseReportSpec
import org.sunbird.analytics.util.EmbeddedPostgresql

class TestOnDemandDruidExhaust extends BaseReportSpec with MockFactory with BaseReportsJob {
    val jobRequestTable = "job_request"
    implicit var spark: SparkSession = _
    implicit var sc: SparkContext = _
    implicit var sqlContext : SQLContext = _
    val outputLocation = AppConf.getConfig("collection.exhaust.store.prefix")
    implicit val fc = new FrameworkContext()
    override def beforeAll(): Unit = {
      super.beforeAll()
      spark = getSparkSession();
      sc = spark.sparkContext
      sqlContext = new SQLContext(sc)
      EmbeddedPostgresql.start()
      EmbeddedPostgresql.createJobRequestTable()
    }

    override def afterAll() : Unit = {
      super.afterAll()
      new HadoopFileUtil().delete(spark.sparkContext.hadoopConfiguration, outputLocation)
      spark.close()
      EmbeddedPostgresql.close()
    }

  "TestOnDemandDruidExhaust" should "generate report with correct values" in {
        EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
        EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('126796199493140000', '888700F9A860E7A42DA968FBECDF3F22', 'ml-task-detail-exhaust', 'SUBMITTED', '{\"type\": \"druid-dataset\",\"params\":{\"programId\" :\"6013eab15faeea0e88a26ef5\",\"state_slug\" : \"up\",\"solutionId\" : \"6065c6c3e9259b7f0b1f5d63\"}}', '36b84ff5-2212-4219-bfed-24886969d890', 'ORG_001', '2021-05-09 19:35:18.666', '{}', NULL, NULL, 0, '' ,0,'test@123');")
        val strConfig = """{"search":{ "type":"none"},"model":"org.sunbird.analytics.exhaust.OnDemandDruidExhaustJob","modelParams":{"reportConfig":{"id":"ml-task-detail-exhaust","queryType":"scan","dateRange":{"staticInterval":"1901-01-01T00:00+00:00/2101-01-01T00:00:00+00:00","granularity":"all"},"metrics":[{"metric":"total_content_plays_on_portal","label":"total_content_plays_on_portal","druidQuery":{"queryType":"scan","dataSource":"dev_ml_project","intervals":"1901-01-01T00:00+00:00/2101-01-01T00:00:00+00:00","columns":["createdBy","designation","state_name","district_name","block_name","school_name","school_externalId","organisation_name","program_name","program_externalId","project_id","project_title_editable","project_description","area_of_improvement","project_duration","tasks","sub_task","task_evidence","task_remarks","state_slug"],"filters":[{"type":"equals","dimension":"private_program","value":"false"},{"type":"equals","dimension":"sub_task_deleted_flag","value":"false"},{"type":"equals","dimension":"task_deleted_flag","value":"false"},{"type":"equals","dimension":"project_deleted_flag","value":"false"},{"type":"equals","dimension":"program_id","value":"$programId"},{"type":"equals","dimension":"solution_id","value":"$solutionId"},{"type":"equals","dimension":"state_slug","value":"$state_slug"}]}}],"labels":{"createdBy":"UUID","designation":"Role","state_name":"Declared State","district_name":"District","block_name":"Block","school_name":"School Name","school_externalId":"School ID","organisation_name":"Organisation Name","program_name":"Program Name","program_externalId":"Program ID","project_id":"Project ID","project_title_editable":"Project Title","project_description":"Project Objective","area_of_improvement":"Category","project_duration":"Project Duration","tasks":"Tasks","sub_task":"Sub-Tasks","task_evidence":"Evidence","task_remarks":"Remarks"},"output":[{"type":"csv","metrics":["createdBy","designation","state_name","district_name","block_name","school_name","school_externalId","organisation_name","program_name","program_externalId","project_id","project_title_editable","project_description","area_of_improvement","project_duration","tasks","sub_task","task_evidence","task_remarks"],"fileParameters":["id","dims"],"zip":false,"dims":["state_slug","date"],"label":""}]},"store":"local","container":"test-container","key":"src/test/resources/ml-druid-data/","format": "csv"},"output":[{"to":"file","params":{"file":"src/test/resources/ml-druid-data-detail"}}],"parallelization":8,"appName":"ML Druid Data Model"}""".stripMargin

        val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
        implicit val config = jobConfig
        implicit val conf = spark.sparkContext.hadoopConfiguration
        OnDemandDruidExhaustJob.execute()
        val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='ml-task-detail-exhaust'")
        while(postgresQuery.next()) {
          postgresQuery.getString("status") should be ("SUCCESS")
          postgresQuery.getString("requested_by") should be ("36b84ff5-2212-4219-bfed-24886969d890")
          postgresQuery.getString("requested_channel") should be ("ORG_001")
          postgresQuery.getString("err_message") should be ("")
          postgresQuery.getString("iteration") should be ("0")
          postgresQuery.getString("encryption_key") should be ("test@123")
        }
  }

    it should "insert status as FAILED in the absence request_data" in {
      EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
      EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('126796199493140000', '888700F9A860E7A42DA968FBECDF3F22', 'ml-task-detail-exhaust', 'SUBMITTED', NULL, '36b84ff5-2212-4219-bfed-24886969d890', 'ORG_001', '2021-05-09 19:35:18.666', NULL, NULL, NULL, 0, 'Invalid request' ,1,'test@123');")
      implicit val fc = new FrameworkContext()
      val strConfig = """{"search":{ "type":"none"},"model":"org.sunbird.analytics.exhaust.OnDemandDruidExhaustJob","modelParams":{"reportConfig":{"id":"ml-task-detail-exhaust","queryType":"scan","dateRange":{"staticInterval":"1901-01-01T00:00+00:00/2101-01-01T00:00:00+00:00","granularity":"all"},"metrics":[{"metric":"total_content_plays_on_portal","label":"total_content_plays_on_portal","druidQuery":{"queryType":"scan","dataSource":"dev_ml_project","intervals":"1901-01-01T00:00+00:00/2101-01-01T00:00:00+00:00","columns":["createdBy","designation","state_name","district_name","block_name","school_name","school_externalId","organisation_name","program_name","program_externalId","project_id","project_title_editable","project_description","area_of_improvement","project_duration","tasks","sub_task","task_evidence","task_remarks","state_slug"],"filters":[{"type":"equals","dimension":"private_program","value":"false"},{"type":"equals","dimension":"sub_task_deleted_flag","value":"false"},{"type":"equals","dimension":"task_deleted_flag","value":"false"},{"type":"equals","dimension":"project_deleted_flag","value":"false"},{"type":"equals","dimension":"program_id","value":"$programId"},{"type":"equals","dimension":"solution_id","value":"$solutionId"},{"type":"equals","dimension":"state_slug","value":"$state_slug"}]}}],"labels":{"createdBy":"UUID","designation":"Role","state_name":"Declared State","district_name":"District","block_name":"Block","school_name":"School Name","school_externalId":"School ID","organisation_name":"Organisation Name","program_name":"Program Name","program_externalId":"Program ID","project_id":"Project ID","project_title_editable":"Project Title","project_description":"Project Objective","area_of_improvement":"Category","project_duration":"Project Duration","tasks":"Tasks","sub_task":"Sub-Tasks","task_evidence":"Evidence","task_remarks":"Remarks"},"output":[{"type":"csv","metrics":["createdBy","designation","state_name","district_name","block_name","school_name","school_externalId","organisation_name","program_name","program_externalId","project_id","project_title_editable","project_description","area_of_improvement","project_duration","tasks","sub_task","task_evidence","task_remarks"],"fileParameters":["id","dims"],"zip":true,"dims":["state_slug","date"],"label":""}]},"store":"local","container":"test-container","key":"src/test/resources/ml-druid-data/","format": "csv"},"output":[{"to":"file","params":{"file":"src/test/resources/ml-druid-data-detail"}}],"parallelization":8,"appName":"ML Druid Data Model"}""".stripMargin
      val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
      implicit val config = jobConfig
      implicit val conf = spark.sparkContext.hadoopConfiguration

      OnDemandDruidExhaustJob.execute()
      val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='ml-task-detail-exhaust'")
      while (postgresQuery.next()) {
        postgresQuery.getString("status") should be ("FAILED")
        postgresQuery.getString("err_message") should be ("Invalid request")
        postgresQuery.getString("download_urls") should be ("{}")
      }
    }

    it should "generate the reports with No Range" in {
      EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
      EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('126796199493140000', '888700F9A860E7A42DA968FBECDF3F22', 'ml-task-detail-exhaust', 'SUBMITTED', '{\"type\": \"druid-dataset\",\"params\":{\"programId\" :\"6013eab15faeea0e88a26ef5\",\"state_slug\" : \"up\",\"solutionId\" : \"6065c6c3e9259b7f0b1f5d63\"}}', '36b84ff5-2212-4219-bfed-24886969d890', 'ORG_001', '2021-05-09 19:35:18.666', '{}', NULL, NULL, 0, '' ,0,'test@123');")

      implicit val fc = new FrameworkContext()
      val strConfig = """{"search":{ "type":"none"},"model":"org.sunbird.analytics.exhaust.OnDemandDruidExhaustJob","modelParams":{"reportConfig":{"id":"ml-task-detail-exhaust","queryType":"scan","dateRange":{"staticInterval":null,"granularity":null},"metrics":[{"metric":"total_content_plays_on_portal","label":"total_content_plays_on_portal","druidQuery":{"queryType":"scan","dataSource":"dev_ml_project","intervals":"1901-01-01T00:00+00:00/2101-01-01T00:00:00+00:00","columns":["createdBy","designation","state_name","district_name","block_name","school_name","school_externalId","organisation_name","program_name","program_externalId","project_id","project_title_editable","project_description","area_of_improvement","project_duration","tasks","sub_task","task_evidence","task_remarks","state_slug"],"filters":[{"type":"equals","dimension":"private_program","value":"false"},{"type":"equals","dimension":"sub_task_deleted_flag","value":"false"},{"type":"equals","dimension":"task_deleted_flag","value":"false"},{"type":"equals","dimension":"project_deleted_flag","value":"false"},{"type":"equals","dimension":"program_id","value":"$programId"},{"type":"equals","dimension":"solution_id","value":"$solutionId"},{"type":"equals","dimension":"state_slug","value":"$state_slug"}]}}],"labels":{"createdBy":"UUID","designation":"Role","state_name":"Declared State","district_name":"District","block_name":"Block","school_name":"School Name","school_externalId":"School ID","organisation_name":"Organisation Name","program_name":"Program Name","program_externalId":"Program ID","project_id":"Project ID","project_title_editable":"Project Title","project_description":"Project Objective","area_of_improvement":"Category","project_duration":"Project Duration","tasks":"Tasks","sub_task":"Sub-Tasks","task_evidence":"Evidence","task_remarks":"Remarks"},"output":[{"type":"csv","metrics":["createdBy","designation","state_name","district_name","block_name","school_name","school_externalId","organisation_name","program_name","program_externalId","project_id","project_title_editable","project_description","area_of_improvement","project_duration","tasks","sub_task","task_evidence","task_remarks"],"fileParameters":["id","dims"],"zip":true,"dims":["state_slug","date"],"label":""}]},"store":"local","container":"test-container","key":"src/test/resources/ml-druid-data/","format": "csv"},"output":[{"to":"file","params":{"file":"src/test/resources/ml-druid-data-detail"}}],"parallelization":8,"appName":"ML Druid Data Model"}""".stripMargin
      val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
      implicit val config = jobConfig
      implicit val conf = spark.sparkContext.hadoopConfiguration
      OnDemandDruidExhaustJob.execute()
    }

    it should "insert status as FAILED  with No Interval" in {
      EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
      EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('126796199493140000', '888700F9A860E7A42DA968FBECDF3F22', 'ml-task-detail-exhaust', 'SUBMITTED', '{\"type\": \"druid-dataset\",\"params\":{\"programId\" :\"6013eab15faeea0e88a26ef5\",\"state_slug\" : \"up\",\"solutionId\" : \"6065c6c3e9259b7f0b1f5d63\"}}', '36b84ff5-2212-4219-bfed-24886969d890', 'ORG_001', '2021-05-09 19:35:18.666', '{}', NULL, NULL, 0, '' ,0,'test@123');")
      implicit val fc = new FrameworkContext()

      val strConfig = """{"search":{ "type":"none"},"model":"org.sunbird.analytics.exhaust.OnDemandDruidExhaustJob","modelParams":{"reportConfig":{"id":"ml-task-detail-exhaust","queryType":"scan","dateRange":{"interval": {"startDate": "1901-01-01","endDate": "2101-01-01"},"granularity":null,"intervalSlider": 0},"metrics":[{"metric":"total_content_plays_on_portal","label":"total_content_plays_on_portal","druidQuery":{"queryType":"scan","dataSource":"dev_ml_project","intervals":"1901-01-01T00:00+00:00/2101-01-01T00:00:00+00:00","columns":["createdBy","designation","state_name","district_name","block_name","school_name","school_externalId","organisation_name","program_name","program_externalId","project_id","project_title_editable","project_description","area_of_improvement","project_duration","tasks","sub_task","task_evidence","task_remarks","state_slug"],"filters":[{"type":"equals","dimension":"private_program","value":"false"},{"type":"equals","dimension":"sub_task_deleted_flag","value":"false"},{"type":"equals","dimension":"task_deleted_flag","value":"false"},{"type":"equals","dimension":"project_deleted_flag","value":"false"},{"type":"equals","dimension":"program_id","value":"$programId"},{"type":"equals","dimension":"solution_id","value":"$solutionId"},{"type":"equals","dimension":"state_slug","value":"$state_slug"}]}}],"labels":{"createdBy":"UUID","designation":"Role","state_name":"Declared State","district_name":"District","block_name":"Block","school_name":"School Name","school_externalId":"School ID","organisation_name":"Organisation Name","program_name":"Program Name","program_externalId":"Program ID","project_id":"Project ID","project_title_editable":"Project Title","project_description":"Project Objective","area_of_improvement":"Category","project_duration":"Project Duration","tasks":"Tasks","sub_task":"Sub-Tasks","task_evidence":"Evidence","task_remarks":"Remarks"},"output":[{"type":"csv","metrics":["createdBy","designation","state_name","district_name","block_name","school_name","school_externalId","organisation_name","program_name","program_externalId","project_id","project_title_editable","project_description","area_of_improvement","project_duration","tasks","sub_task","task_evidence","task_remarks"],"fileParameters":["id","dims"],"zip":false,"dims":["state_slug","date"],"label":""}]},"store":"local","container":"test-container","key":"src/test/resources/ml-druid-data/","format": "csv","quoteColumns": ["Role","Declared State","District","Block","School Name","Organisation Name","Program Name","Project Title","Project Objective","Category","Tasks","Sub-Tasks","Remarks"]},"output":[{"to":"file","params":{"file":"src/test/resources/ml-druid-data-detail"}}],"parallelization":8,"appName":"ML Druid Data Model"}""".stripMargin
      val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
      implicit val config = jobConfig
      implicit val conf = spark.sparkContext.hadoopConfiguration
      OnDemandDruidExhaustJob.execute()
    }

    it should "insert status as SUCCESS encryption key not provided" in {
      EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
      EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('126796199493140000', '888700F9A860E7A42DA968FBECDF3F22', 'ml-task-detail-exhaust', 'SUBMITTED', '{\"type\": \"druid-dataset\",\"params\":{\"programId\" :\"6013eab15faeea0e88a26ef5\",\"state_slug\" : \"up\",\"solutionId\" : \"6065c6c3e9259b7f0b1f5d63\"}}', '36b84ff5-2212-4219-bfed-24886969d890', 'ORG_001', '2021-05-09 19:35:18.666', '{src/test/resources/ml-druid-data/ml-task-detail-exhaust/1626335633616_888700F9A860E7A42DA968FBECDF3F22.csv}', NULL, NULL, 0, '' ,0,NULL);")

      implicit val fc = new FrameworkContext()
      val strConfig = """{"search":{ "type":"none"},"model":"org.sunbird.analytics.exhaust.OnDemandDruidExhaustJob","modelParams":{"reportConfig":{"id":"ml-task-detail-exhaust","queryType":"scan","dateRange":{"staticInterval":"1901-01-01T00:00+00:00/2101-01-01T00:00:00+00:00","granularity":"all"},"metrics":[{"metric":"total_content_plays_on_portal","label":"total_content_plays_on_portal","druidQuery":{"queryType":"scan","dataSource":"dev_ml_project","intervals":"1901-01-01T00:00+00:00/2101-01-01T00:00:00+00:00","columns":["createdBy","designation","state_name","district_name","block_name","school_name","school_externalId","organisation_name","program_name","program_externalId","project_id","project_title_editable","project_description","area_of_improvement","project_duration","tasks","sub_task","task_evidence","task_remarks","state_slug"],"filters":[{"type":"equals","dimension":"private_program","value":"false"},{"type":"equals","dimension":"sub_task_deleted_flag","value":"false"},{"type":"equals","dimension":"task_deleted_flag","value":"false"},{"type":"equals","dimension":"project_deleted_flag","value":"false"},{"type":"equals","dimension":"program_id","value":"$programId"},{"type":"equals","dimension":"solution_id","value":"$solutionId"},{"type":"equals","dimension":"state_slug","value":"$state_slug"}]}}],"labels":{"createdBy":"UUID","designation":"Role","state_name":"Declared State","district_name":"District","block_name":"Block","school_name":"School Name","school_externalId":"School ID","organisation_name":"Organisation Name","program_name":"Program Name","program_externalId":"Program ID","project_id":"Project ID","project_title_editable":"Project Title","project_description":"Project Objective","area_of_improvement":"Category","project_duration":"Project Duration","tasks":"Tasks","sub_task":"Sub-Tasks","task_evidence":"Evidence","task_remarks":"Remarks"},"output":[{"type":"csv","metrics":["createdBy","designation","state_name","district_name","block_name","school_name","school_externalId","organisation_name","program_name","program_externalId","project_id","project_title_editable","project_description","area_of_improvement","project_duration","tasks","sub_task","task_evidence","task_remarks"],"fileParameters":["id","dims"],"zip":false,"dims":["state_slug","date"],"label":""}]},"store":"local","container":"test-container","key":"src/test/resources/ml-druid-data/","format": "csv","quoteColumns": ["Role","Declared State","District","Block","School Name","Organisation Name","Program Name","Project Title","Project Objective","Category","Tasks","Sub-Tasks","Remarks"]},"output":[{"to":"file","params":{"file":"src/test/resources/ml-druid-data-detail"}}],"parallelization":8,"appName":"ML Druid Data Model"}""".stripMargin
      val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
      implicit val config = jobConfig
      implicit val conf = spark.sparkContext.hadoopConfiguration
      OnDemandDruidExhaustJob.execute()

      val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='ml-task-detail-exhaust'")
      while (postgresQuery.next()) {
        postgresQuery.getString("status") should be ("SUCCESS")
        postgresQuery.getString("err_message") should be ("")
      }
    }

    it should "execute the update and save request method" in {
      implicit val fc = new FrameworkContext()
      val jobRequest = JobRequest("126796199493140000", "888700F9A860E7A42DA968FBECDF3F22", "ml-task-detail-exhaust", "SUBMITTED", "{\"type\": \"druid-dataset\",\"params\":{\"programId\" :\"6013eab15faeea0e88a26ef5\",\"state_slug\" : \"up\",\"solutionId\" : \"6065c6c3e9259b7f0b1f5d63\"}}", "36b84ff5-2212-4219-bfed-24886969d890", "ORG_001", System.currentTimeMillis(), None, None, None, Option(0), Option("") ,Option(0),Option("test@123"))
      val req = new JobRequest()
      val jobRequestArr = Array(jobRequest)
      val storageConfig = StorageConfig("local", "", outputLocation)
      implicit val conf = spark.sparkContext.hadoopConfiguration

      OnDemandDruidExhaustJob.saveRequests(storageConfig, jobRequestArr)
    }

    it should "generate the report with quote column" in {
      EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
      EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('126796199493140000', '888700F9A860E7A42DA968FBECDF3F22', 'ml-task-detail-exhaust', 'SUBMITTED', '{\"type\": \"druid-dataset\",\"params\":{\"programId\" :\"6013eab15faeea0e88a26ef5\",\"state_slug\" : \"up\",\"solutionId\" : \"6065c6c3e9259b7f0b1f5d63\"}}', '36b84ff5-2212-4219-bfed-24886969d890', 'ORG_001', '2021-05-09 19:35:18.666', '{}', NULL, NULL, 0, '' ,0,'test@123');")
      implicit val fc = new FrameworkContext()
      val strConfig = """{"search":{ "type":"none"},"model":"org.sunbird.analytics.exhaust.OnDemandDruidExhaustJob","modelParams":{"reportConfig":{"id":"ml-task-detail-exhaust","queryType":"scan","dateRange":{"staticInterval":"1901-01-01T00:00+00:00/2101-01-01T00:00:00+00:00","granularity":"all"},"metrics":[{"metric":"total_content_plays_on_portal","label":"total_content_plays_on_portal","druidQuery":{"queryType":"scan","dataSource":"dev_ml_project","intervals":"1901-01-01T00:00+00:00/2101-01-01T00:00:00+00:00","columns":["createdBy","designation","state_name","district_name","block_name","school_name","school_externalId","organisation_name","program_name","program_externalId","project_id","project_title_editable","project_description","area_of_improvement","project_duration","tasks","sub_task","task_evidence","task_remarks","state_slug"],"filters":[{"type":"equals","dimension":"private_program","value":"false"},{"type":"equals","dimension":"sub_task_deleted_flag","value":"false"},{"type":"equals","dimension":"task_deleted_flag","value":"false"},{"type":"equals","dimension":"project_deleted_flag","value":"false"},{"type":"equals","dimension":"program_id","value":"$programId"},{"type":"equals","dimension":"solution_id","value":"$solutionId"},{"type":"equals","dimension":"state_slug","value":"$state_slug"}]}}],"labels":{"createdBy":"UUID","designation":"Role","state_name":"Declared State","district_name":"District","block_name":"Block","school_name":"School Name","school_externalId":"School ID","organisation_name":"Organisation Name","program_name":"Program Name","program_externalId":"Program ID","project_id":"Project ID","project_title_editable":"Project Title","project_description":"Project Objective","area_of_improvement":"Category","project_duration":"Project Duration","tasks":"Tasks","sub_task":"Sub-Tasks","task_evidence":"Evidence","task_remarks":"Remarks"},"output":[{"type":"csv","metrics":["createdBy","designation","state_name","district_name","block_name","school_name","school_externalId","organisation_name","program_name","program_externalId","project_id","project_title_editable","project_description","area_of_improvement","project_duration","tasks","sub_task","task_evidence","task_remarks"],"fileParameters":["id","dims"],"zip":false,"dims":["state_slug","date"],"label":""}]},"store":"local","container":"test-container","key":"src/test/resources/ml-druid-data/","format": "csv","quoteColumns": ["Role","Declared State","District","Block","School Name","Organisation Name","Program Name","Project Title","Project Objective","Category","Tasks","Sub-Tasks","Remarks"]},"output":[{"to":"file","params":{"file":"src/test/resources/ml-druid-data-detail"}}],"parallelization":8,"appName":"ML Druid Data Model"}""".stripMargin
      val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
      implicit val config = jobConfig
      implicit val conf = spark.sparkContext.hadoopConfiguration
      OnDemandDruidExhaustJob.execute()
    }

    it should "generate the report  with no label" in {
      EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
      EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('126796199493140000', '888700F9A860E7A42DA968FBECDF3F22', 'ml-task-detail-exhaust', 'SUBMITTED', '{\"type\": \"druid-dataset\",\"params\":{\"programId\" :\"6013eab15faeea0e88a26ef5\",\"state_slug\" : \"up\",\"solutionId\" : \"6065c6c3e9259b7f0b1f5d63\"}}', '36b84ff5-2212-4219-bfed-24886969d890', 'ORG_001', '2021-05-09 19:35:18.666', '{}', NULL, NULL, 0, '' ,0,'test@123');")
      implicit val fc = new FrameworkContext()
      val strConfig = """{"search":{ "type":"none"},"model":"org.sunbird.analytics.exhaust.OnDemandDruidExhaustJob","modelParams":{"reportConfig":{"id":"ml-task-detail-exhaust","queryType":"scan","dateRange":{"staticInterval":"1901-01-01T00:00+00:00/2101-01-01T00:00:00+00:00","granularity":"all"},"metrics":[],"labels":{},"output":[{"type":"csv","metrics":[],"fileParameters":["id","dims"],"zip":false,"dims":["state_slug","date"],"label":""}]},"store":"local","container":"test-container","key":"src/test/resources/ml-druid-data/","format": "csv"},"output":[{"to":"file","params":{"file":"src/test/resources/ml-druid-data-detail"}}],"parallelization":8,"appName":"ML Druid Data Model"}""".stripMargin
      val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
      implicit val config = jobConfig
      implicit val conf = spark.sparkContext.hadoopConfiguration
      OnDemandDruidExhaustJob.execute()
    }

    it should "insert status as failed when filter doesn't match" in {
      EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
      EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('126796199493140000', '888700F9A860E7A42DA968FBECDF3F22', 'ml-task-detail-exhaust', 'SUBMITTED', '{\"type\": \"druid-dataset\",\"params\":{\"programId\" :\"6013eab15faeea0e88a26ef5\",\"state_slug\" : \"up\",\"solutionId\" : \"6065c6c3e9259b7f0b1f445d63\"}}', '36b84ff5-2212-4219-bfed-24886969d890', 'ORG_001', '2021-05-09 19:35:18.666', '{}', NULL, NULL, 0, 'No data found from druid' ,0,'test@123');")
      val strConfig = """{"search":{ "type":"none"},"model":"org.sunbird.analytics.exhaust.OnDemandDruidExhaustJob","modelParams":{"reportConfig":{"id":"ml-task-detail-exhaust","queryType":"scan","dateRange":{"staticInterval":"1901-01-01T00:00+00:00/2101-01-01T00:00:00+00:00","granularity":"all"},"metrics":[{"metric":"total_content_plays_on_portal","label":"total_content_plays_on_portal","druidQuery":{"queryType":"scan","dataSource":"dev_ml_project","intervals":"1901-01-01T00:00+00:00/2101-01-01T00:00:00+00:00","columns":["createdBy","designation","state_name","district_name","block_name","school_name","school_externalId","organisation_name","program_name","program_externalId","project_id","project_title_editable","project_description","area_of_improvement","project_duration","tasks","sub_task","task_evidence","task_remarks","state_slug"],"filters":[{"type":"equals","dimension":"private_program","value":"false"},{"type":"equals","dimension":"sub_task_deleted_flag","value":"false"},{"type":"equals","dimension":"task_deleted_flag","value":"false"},{"type":"equals","dimension":"project_deleted_flag","value":"false"},{"type":"equals","dimension":"program_id","value":"$programId"},{"type":"equals","dimension":"solution_id","value":"$solutionId"},{"type":"equals","dimension":"state_slug","value":"$state_slug"}]}}],"labels":{"createdBy":"UUID","designation":"Role","state_name":"Declared State","district_name":"District","block_name":"Block","school_name":"School Name","school_externalId":"School ID","organisation_name":"Organisation Name","program_name":"Program Name","program_externalId":"Program ID","project_id":"Project ID","project_title_editable":"Project Title","project_description":"Project Objective","area_of_improvement":"Category","project_duration":"Project Duration","tasks":"Tasks","sub_task":"Sub-Tasks","task_evidence":"Evidence","task_remarks":"Remarks"},"output":[{"type":"csv","metrics":["createdBy","designation","state_name","district_name","block_name","school_name","school_externalId","organisation_name","program_name","program_externalId","project_id","project_title_editable","project_description","area_of_improvement","project_duration","tasks","sub_task","task_evidence","task_remarks"],"fileParameters":["id","dims"],"zip":false,"dims":["state_slug","date"],"label":""}]},"store":"local","container":"test-container","key":"src/test/resources/ml-druid-data/","format": "csv"},"output":[{"to":"file","params":{"file":"src/test/resources/ml-druid-data-detail"}}],"parallelization":8,"appName":"ML Druid Data Model"}""".stripMargin

      val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
      implicit val config = jobConfig
      implicit val conf = spark.sparkContext.hadoopConfiguration
      OnDemandDruidExhaustJob.execute()
      val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='ml-task-detail-exhaust'")
      while(postgresQuery.next()) {
        postgresQuery.getString("status") should be ("FAILED")
        postgresQuery.getString("err_message") should be ("No data found from druid")
      }
    }

    it should "generate report with other generic query" in {
      EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
      EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('126796199493140000', '999700F9A860E7A42DA968FBECDF3F22', 'ml-obs-question-detail-exhaust', 'SUBMITTED', '{\"type\": \"druid-dataset\",\"params\":{\"programId\" :\"60549338acf1c71f0b2409c3\",\"state_slug\" : \"apekx\",\"solutionId\" : \"605c934eda9dea6400302afc\"}}', '36b84ff5-2212-4219-bfed-24886969d890', 'ORG_001', '2021-07-15 19:35:18.666', '{}', NULL, NULL, 0, '' ,0,'demo@123');")
      val strConfig = """{"search":{ "type":"none"},"model":"org.sunbird.analytics.exhaust.OnDemandDruidExhaustJob","modelParams":{"reportConfig":{"id":"ml-obs-question-detail-exhaust","queryType":"scan","dateRange":{"staticInterval":"1901-01-01T00:00+00:00/2101-01-01T00:00:00+00:00","granularity":"all"},"metrics":[{"metric":"total_content_plays_on_portal","label":"total_content_plays_on_portal","druidQuery":{"queryType":"scan","dataSource":"dev_ml_observation","intervals":"1901-01-01T00:00+00:00/2101-01-01T00:00:00+00:00","columns":["createdBy","role_title","user_stateName","user_districtName","user_blockName","user_schoolName","user_schoolUDISE_code","organisation_name","programName","programExternalId","solutionName","solutionExternalId","observationSubmissionId","questionExternalId","questionName","questionResponseLabel","minScore","evidences","remarks","state_slug"],"filters":[{"type":"equals","dimension":"isAPrivateProgram","value":"false"},{"type":"equals","dimension":"programId","value":"$programId"},{"type":"equals","dimension":"solutionId","value":"$solutionId"},{"type":"equals","dimension":"state_slug","value":"$state_slug"}]}}],"labels":{"createdBy": "UUID","role_title": "User Sub Type","user_stateName": "Declared State","user_districtName": "District","user_blockName": "Block","user_schoolName": "School Name","user_schoolUDISE_code": "School ID","organisation_name": "Organisation Name","programName": "Program Name","programExternalId": "Program ID","solutionName": "Observation Name","solutionExternalId": "Observation ID","observationSubmissionId": "observation_submission_id","questionExternalId": "Question_external_id","questionName": "Question","questionResponseLabel": "Question_response_label", "minScore": "Question score","remarks": "Remarks"},"output":[{"type":"csv","metrics":["createdBy","role_title","user_stateName","user_districtName","user_blockName","user_schoolName","user_schoolUDISE_code","organisation_name","programName","programExternalId","solutionName","solutionExternalId","observationSubmissionId","questionExternalId","questionName","questionResponseLabel","minScore","evidences","remarks"],"fileParameters":["id","dims"],"zip":false,"dims":["state_slug","date"],"label":""}]},"store":"local","container":"test-container","key":"src/test/resources/ml-druid-data/","format": "csv"},"output":[{"to":"file","params":{"file":"src/test/resources/ml-druid-data-detail"}}],"parallelization":8,"appName":"ML Druid Data Model"}""".stripMargin

      val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
      implicit val config = jobConfig
      implicit val conf = spark.sparkContext.hadoopConfiguration
      OnDemandDruidExhaustJob.execute()
      val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='ml-obs-question-detail-exhaust'")
      while(postgresQuery.next()) {
        postgresQuery.getString("status") should be ("SUCCESS")
        postgresQuery.getString("requested_by") should be ("36b84ff5-2212-4219-bfed-24886969d890")
        postgresQuery.getString("requested_channel") should be ("ORG_001")
        postgresQuery.getString("err_message") should be ("")
        postgresQuery.getString("iteration") should be ("0")
        postgresQuery.getString("encryption_key") should be ("demo@123")
      }
    }
}