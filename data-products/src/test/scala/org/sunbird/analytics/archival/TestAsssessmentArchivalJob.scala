package org.sunbird.analytics.archival

import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.ekstep.analytics.framework.util.JSONUtils
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.exhaust.BaseReportsJob
import org.sunbird.analytics.job.report.BaseReportSpec
import org.sunbird.analytics.util.{EmbeddedCassandra, EmbeddedPostgresql}

class TestAsssessmentArchivalJob extends BaseReportSpec with MockFactory with BaseReportsJob {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = getSparkSession();
    super.beforeAll()
    EmbeddedCassandra.loadData("src/test/resources/exhaust/report_data.cql") // Load test data in embedded cassandra server
    EmbeddedPostgresql.start()
    EmbeddedPostgresql.createArchivalRequestTable()
  }

  override def afterAll() : Unit = {
    super.afterAll()
    EmbeddedCassandra.close()
    EmbeddedPostgresql.close()
    spark.close()
  }

  "AssessmentArchivalJob" should "archive the batch which is not archived in past" in {
    implicit val fc = new FrameworkContext()

    val strConfig= """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.$job_name","modelParams":{"mode":"archival","request":{"archivalTable":"assessment_aggregator","query":"{}","batchId":"batch-001","date":"2021-11-01"},"blobConfig":{"store":"azure","blobExt":"csv.gz","reportPath":"assessment-archived-data/","container":"reports"},"sparkCassandraConnectionHost":"{{ core_cassandra_host }}","fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')"},"parallelization":8,"appName":"$job_name"}"""
    implicit val jobConfig= JSONUtils.deserialize[JobConfig](strConfig)

    AssessmentArchivalJob.execute()
  }

  it should "archive the batch which is archived in past" in {
    implicit val fc = new FrameworkContext()

    EmbeddedPostgresql.execute(s"TRUNCATE archival_metadata")
    EmbeddedPostgresql.execute("INSERT INTO archival_metadata (request_id, batch_id, collection_id , resource_type , job_id , archival_date, completion_date, archival_status, blob_url, iteration,request_data , err_message ) VALUES ('do_1130928636168192001667_batch-001', 'batch-001', 'do_1130928636168192001667', 'assessment', 'assessment-archival','2020-10-19 05:58:18.666','2020-10-19 05:58:18.666','SUCCESS', '{\"reports/assessment-archival/batch-001/20-2019.csv.gz\"}', 1,'{\"batchId\": \"batch-001\"}', NULL);")

    val strConfig= """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.$job_name","modelParams":{"mode":"archival","request":{"archivalTable":"assessment_aggregator","query":"{}","batchId":"batch-001","date":"2021-11-01"},"blobConfig":{"store":"azure","blobExt":"csv.gz","reportPath":"assessment-archived-data/","container":"reports"},"sparkCassandraConnectionHost":"{{ core_cassandra_host }}","fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')"},"parallelization":8,"appName":"$job_name"}"""
    implicit val jobConfig= JSONUtils.deserialize[JobConfig](strConfig)

    AssessmentArchivalJob.execute()
  }

}
