package org.sunbird.analytics.audit

import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.{SomeColumns, toRDDFunctions}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig}
import org.sunbird.analytics.exhaust.BaseReportsJob

import java.io._
import scala.collection.immutable.List

case class ContentResponse(result: ContentResult, responseCode: String)

case class ContentResult(content: Map[String, AnyRef])

case class AssessmentCorrectionMetrics(batchId: String, contentId: String, invalidRecords: Long, totalAffectedUsers: Long, contentTotalQuestions: Long)

case class ContentMeta(totalQuestions: Int, contentType: String, contentId: String)


object AssessmentScoreCorrectionJob extends optional.Application with IJob with BaseReportsJob {
  implicit val className: String = "org.sunbird.analytics.audit.AssessmentScoreCorrectionJob"
  val cassandraFormat = "org.apache.spark.sql.cassandra"
  private val assessmentAggDBSettings = Map("table" -> "assessment_aggregator", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster")
  private val userEnrolmentDBSettings = Map("table" -> "user_enrolments", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "ReportCluster")

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
      spark.setCassandraConf("ReportCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.report.cluster.host")))
      val res = CommonUtil.time(processBatches())
      JobLogger.end(s"$jobName completed execution", "SUCCESS", Option(Map("time_taken" -> res._1, "processed_batches" -> res._2)))
    } catch {
      case ex: Exception =>
        JobLogger.log(ex.getMessage, None, ERROR)
        JobLogger.end(s"$jobName execution failed", "FAILED", Option(Map("model" -> jobName, "statusMsg" -> ex.getMessage)));
    }
    finally {
      frameworkContext.closeContext()
      spark.close()
    }
  }

  // $COVERAGE-ON$ Enabling scoverage
  def processBatches()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig, sc: SparkContext): List[List[AssessmentCorrectionMetrics]] = {
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]())
    val batchIds: List[String] = modelParams.getOrElse("assessment.score.correction.batches", List()).asInstanceOf[List[String]].filter(x => x.nonEmpty)
    val isDryRunMode = modelParams.getOrElse("isDryRunMode", true).asInstanceOf[Boolean]
    for (batchId <- batchIds) yield {
      JobLogger.log("Started Processing the Batch", Option(Map("batch_id" -> batchId, "isDryRunMode" -> isDryRunMode)), INFO)
      process(batchId = batchId, isDryRunMode = isDryRunMode)
    }
  }

  def process(batchId: String,
              isDryRunMode: Boolean)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig, sc: SparkContext): List[AssessmentCorrectionMetrics] = {
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]())
    val assessmentData: DataFrame = getAssessmentAggData(batchId = batchId)
      .withColumn("question_data", explode_outer(col("question")))
      .withColumn("question_id", col("question_data.id"))
      .select("course_id", "batch_id", "content_id", "attempt_id", "user_id", "question_id")
      .groupBy("course_id", "batch_id", "content_id", "attempt_id", "user_id").agg(size(collect_list("question_id")).as("total_question")).persist()
    val userEnrolmentDF = getUserEnrolment(batchId = batchId).persist()
    val contentIds: List[String] = assessmentData.select("content_id").distinct().collect().map(_ (0)).toList.asInstanceOf[List[String]]
    val res = for (contentId <- contentIds) yield {
      val contentMetaURL: String = modelParams.getOrElse("contentReadAPI", "https://diksha.gov.in/api/content/v1/read/").asInstanceOf[String]
      val supportedContentType: String = modelParams.getOrElse("supportedContentType", "SelfAssess").asInstanceOf[String]
      val contentMeta: ContentMeta = getTotalQuestions(contentId, contentMetaURL)
      JobLogger.log("Fetched the content meta value to the processing batch", Option(contentMeta), INFO)
      if (StringUtils.equals(contentMeta.contentType, supportedContentType)) {
        correctData(assessmentData, userEnrolmentDF, batchId, contentMeta.contentId, contentMeta.totalQuestions, isDryRunMode)
      } else {
        JobLogger.log("The content ID is not self assess, Skipping data removal process", Some(Map("contentId" -> contentId, "contentType" -> contentMeta.contentType)), INFO)
        AssessmentCorrectionMetrics(batchId = batchId, contentId = contentId, invalidRecords = 0, totalAffectedUsers = 0, contentTotalQuestions = contentMeta.totalQuestions)
      }
    }
    userEnrolmentDF.unpersist()
    res
  }

  def correctData(assessmentDF: DataFrame,
                  userEnrolDF: DataFrame,
                  batchId: String,
                  contentId: String,
                  totalQuestions: Int,
                  isDryRunMode: Boolean
                 )(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig, sc: SparkContext): AssessmentCorrectionMetrics = {
    val incorrectRecords: DataFrame = assessmentDF.filter(col("content_id") === contentId).filter(col("total_question") =!= totalQuestions)
      .select("course_id", "batch_id", "content_id", "attempt_id", "user_id")
    val incorrectRecordsSize: Long = incorrectRecords.count()
    val metrics = AssessmentCorrectionMetrics(batchId = batchId, contentId = contentId, invalidRecords = incorrectRecordsSize, totalAffectedUsers = incorrectRecords.select("user_id").distinct().count(), contentTotalQuestions = totalQuestions)
    JobLogger.log("Total Incorrect Records", Option(metrics), INFO)
    if (incorrectRecordsSize > 0) {
      removeAssessmentRecords(incorrectRecords, batchId, contentId, isDryRunMode)
      generateInstructionEvents(incorrectRecords, batchId, contentId)
      saveRevokingCertIds(incorrectRecords, userEnrolDF, batchId, contentId)
    }
    metrics
  }


  def saveRevokingCertIds(incorrectRecords: DataFrame,
                          userEnrolmentDF: DataFrame,
                          batchId: String,
                          contentId: String)(implicit config: JobConfig): Unit = {
    val certIdsDF: DataFrame = incorrectRecords.select("course_id", "batch_id", "content_id", "user_id").distinct()
      .join(userEnrolmentDF, incorrectRecords.col("user_id") === userEnrolmentDF.col("userid") && incorrectRecords.col("course_id") === userEnrolmentDF.col("courseid") &&
        incorrectRecords.col("batch_id") === userEnrolmentDF.col("batchid"), "left_outer")
      .withColumn("certificate_data", explode_outer(col("issued_certificates")))
      .withColumn("certificate_id", col("certificate_data.identifier"))
      .select("courseid", "batchid", "userid", "certificate_id").filter(col("certificate_id") =!= "")
    saveLocal(certIdsDF, batchId, contentId = contentId, "revoked-cert-data")
  }

  def saveLocal(data: DataFrame,
                batchId: String,
                contentId: String,
                folderName: String)(implicit config: JobConfig): Unit = {
    JobLogger.log("Generating the CSV File", Option(Map("batch_id" -> batchId, "content_id" -> contentId)), INFO)
    val outputPath = config.modelParams.getOrElse(Map[String, Option[AnyRef]]()).getOrElse("csvPath", "").asInstanceOf[String]
    data.repartition(1).write.option("header", value = true).format("com.databricks.spark.csv").save(outputPath.concat(s"/$folderName-$batchId-$contentId-${System.currentTimeMillis()}.csv"))
  }


  def removeAssessmentRecords(incorrectRecords: DataFrame,
                              batchId: String,
                              contentId: String,
                              isDryRunMode: Boolean)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig, sc: SparkContext): Unit = {
    if (isDryRunMode) {
      saveLocal(incorrectRecords, batchId, contentId, "assessment-invalid-attempts-records")
    } else {
      JobLogger.log("Deleting the records from the table", None, INFO)
      saveLocal(incorrectRecords, batchId, contentId, "assessment-invalid-attempts-records") // For cross verification purpose
      incorrectRecords.select("course_id", "batch_id", "user_id", "content_id", "attempt_id").rdd.deleteFromCassandra(AppConf.getConfig("sunbird.courses.keyspace"), "assessment_aggregator", keyColumns = SomeColumns("course_id", "batch_id", "user_id", "content_id", "attempt_id"))
    }
  }

  def generateInstructionEvents(inCorrectRecordsDF: DataFrame,
                                batchId: String,
                                contentId: String)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig, sc: SparkContext): Unit = {
    val outputPath = config.modelParams.getOrElse(Map[String, Option[AnyRef]]()).getOrElse("csvPath", "").asInstanceOf[String]
    val file = new File(outputPath.concat(s"/instruction-events-$batchId-$contentId-${System.currentTimeMillis()}.json"))
    val writer: BufferedWriter = new BufferedWriter(new FileWriter(file))
    try {
      val userIds: List[String] = inCorrectRecordsDF.select("user_id").distinct().collect().map(_ (0)).toList.asInstanceOf[List[String]]
      val courseId: String = inCorrectRecordsDF.select("course_id").distinct().head.getString(0)
      for (userId <- userIds) yield {
        val event = s"""{"assessmentTs":${System.currentTimeMillis()},"batchId":"$batchId","courseId":"$courseId","userId":"$userId","contentId":"$contentId"}"""
        writer.write(event)
        writer.newLine()
      }
      writer.flush()
    } catch {
      case ex: IOException => ex.printStackTrace()
    } finally {
      writer.close()
    }
  }

  // Method to fetch the totalQuestion count from the content meta
  def getTotalQuestions(contentId: String,
                        apiUrl: String): ContentMeta = {
    val response = RestUtil.get[ContentResponse](apiUrl.concat(contentId))
    if (null != response && response.responseCode.equalsIgnoreCase("ok") && null != response.result.content && response.result.content.nonEmpty) {
      val totalQuestions: Int = response.result.content.getOrElse("totalQuestions", 0).asInstanceOf[Int]
      val contentType: String = response.result.content.getOrElse("contentType", null).asInstanceOf[String]
      ContentMeta(totalQuestions, contentType, contentId)
    } else {
      throw new Exception(s"Failed to fetch the content meta for the content ID: $contentId") // Job should stop if the api has failed
    }
  }

  // Start of fetch logic from the DB
  def getAssessmentAggData(batchId: String)(implicit spark: SparkSession): DataFrame = {
    loadData(assessmentAggDBSettings, cassandraFormat, new StructType())
      .filter(col("batch_id") === batchId)
  }

  def getUserEnrolment(batchId: String)(implicit spark: SparkSession): DataFrame = {
    loadData(userEnrolmentDBSettings, cassandraFormat, new StructType()).select("batchid", "courseid", "userid", "issued_certificates")
      .filter(col("batchid") === batchId)
  }

  // End of fetch logic from the DB
}
