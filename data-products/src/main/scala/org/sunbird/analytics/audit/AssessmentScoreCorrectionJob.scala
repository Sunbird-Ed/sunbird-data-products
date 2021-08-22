package org.sunbird.analytics.audit

import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.toRDDFunctions
import org.apache.spark.SparkContext
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig}
import org.sunbird.analytics.job.report.BaseReportsJob

import scala.collection.immutable.List
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

case class ContentResponse(result: ContentResult, responseCode: String)

case class ContentResult(content: Map[String, AnyRef])

object AssessmentScoreCorrectionJob extends optional.Application with IJob with BaseReportsJob {
  implicit val className: String = "org.sunbird.analytics.audit.AssessmentScoreCorrectionJob"
  val cassandraFormat = "org.apache.spark.sql.cassandra"
  private val assessmentAggDBSettings = Map("table" -> "assessment_aggregator", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster")

  // $COVERAGE-OFF$ Disabling scoverage for main and execute method
  override def main(config: String)(implicit sc: Option[SparkContext], fc: Option[FrameworkContext]): Unit = {
    val jobName: String = "AssessmentScoreCorrectionJob"
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](config)
    JobLogger.init(jobName)
    JobLogger.start(s"$jobName started executing", Option(Map("config" -> config, "model" -> jobName)))
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()
    implicit val spark: SparkSession = openSparkSession(jobConfig)
    implicit val sc: SparkContext = spark.sparkContext
    try {
      spark.setCassandraConf("LMSCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.courses.cluster.host")))
      val res = CommonUtil.time(processBatches())
      JobLogger.end(s"$jobName completed execution", "SUCCESS", Option(Map("time_taken" -> res._1, "processed_batches" -> res._2)))
    } catch {
      case ex: Exception =>
        JobLogger.log(ex.getMessage, None, ERROR);
        JobLogger.end(s"$jobName execution failed", "FAILED", Option(Map("model" -> jobName, "statusMsg" -> ex.getMessage)));
    }
    finally {
      frameworkContext.closeContext()
      spark.close()
    }
  }

  // $COVERAGE-ON$ Enabling scoverage
  def processBatches()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig, sc: SparkContext): List[Map[String, Any]] = {
    val batchIds: List[String] = AppConf.getConfig("assessment.score.correction.batches").split(",").toList.filter(x => x.nonEmpty)
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]())
    val isDryRunMode = modelParams.getOrElse("isDryRunMode", false).asInstanceOf[Boolean]
    for (batchId <- batchIds) yield {
      JobLogger.log("Started Correcting the Max Score Value for the Batch", Option(Map("batch_id" -> batchId, "isDryRunMode" -> isDryRunMode)), INFO)
      removeRecords(batchId = batchId, isDryRunMode = isDryRunMode)
    }
  }

  def removeRecords(batchId: String, isDryRunMode: Boolean)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig, sc: SparkContext) = {
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]())
    val outputPath = modelParams.getOrElse("csvPath", "").asInstanceOf[String]
    // Get the Assessment Data for the specific Batch
    val assessmentData: DataFrame = getAssessmentAggData(batchId).select("course_id", "batch_id", "content_id", "attempt_id", "user_id", "total_max_score", "question")
    // Take the content Id which is associated to the batch being invoked for the correction
    val contentId: String = assessmentData.select("content_id").collect().map(_ (0)).toList.head.asInstanceOf[String]

    val contentMetaURL: String = modelParams.getOrElse("contentReadAPI", "https://diksha.gov.in/api/content/v1/read/").asInstanceOf[String]
    // Get the TotalQuestion Value from the Content Meta API
    val totalQuestions: Int = Await.result[Int](getTotalQuestions(contentId, contentMetaURL), 60.seconds)
    JobLogger.log("Fetched the total questions value for the processing batch", Option(Map("batch_id" -> batchId, "content_id" -> contentId, "total_questions" -> totalQuestions)), INFO)
    val filteredAssessmentData = assessmentData.filter(col("total_max_score") =!= totalQuestions)
      .select("course_id", "batch_id", "content_id", "attempt_id", "user_id")
    val totalRecords = filteredAssessmentData.count()
    JobLogger.log("Total Incorrect Records", Option(Map("batch_id" -> batchId, "total_records" -> totalRecords), INFO))
    if (isDryRunMode) {
      filteredAssessmentData.repartition(1).write.option("header", true).format("com.databricks.spark.csv").save(outputPath.concat(s"/corrected-report-$batchId-${System.currentTimeMillis()}.csv"))
    } else {
      filteredAssessmentData.rdd.deleteFromCassandra(AppConf.getConfig("sunbird.courses.keyspace"), AppConf.getConfig("sunbird.courses.assessment.table"))
    }
    Map("batch_id" -> batchId, "total_records" -> totalRecords, "content_id" -> contentId, "total_questions" -> totalQuestions)

  }

  // Fetch the assessment data for a specific batch identifier
  def getAssessmentAggData(batchId: String)(implicit spark: SparkSession): DataFrame = {
    fetchData(spark, assessmentAggDBSettings, cassandraFormat, new StructType())
      .filter(col("batch_id") === batchId).persist()
  }


  def getTotalQuestions(contentId: String, apiUrl: String) = {
    Future {
      val response = RestUtil.get[ContentResponse](apiUrl.concat(contentId))
      if (null != response && response.responseCode.equalsIgnoreCase("ok") && null != response.result.content && response.result.content.nonEmpty) {
        response.result.content.getOrElse("totalQuestions", 0).asInstanceOf[Int]
      } else {
        0
      }
    }
  }
}
