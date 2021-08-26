package org.sunbird.analytics.audit

import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.toRDDFunctions
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkContext, sql}
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
  private val userActivityAggDBSettings = Map("table" -> "user_activity_agg", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster")

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
  def processBatches()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig, sc: SparkContext): List[List[Map[String, Any]]] = {
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]())
    val batchIds: List[String] = modelParams.getOrElse("assessment.score.correction.batches", List()).asInstanceOf[List[String]].filter(x => x.nonEmpty)
    val isDryRunMode = modelParams.getOrElse("isDryRunMode", true).asInstanceOf[Boolean]
    val correctRawAssessment = modelParams.getOrElse("correctRawAssessment", true).asInstanceOf[Boolean]
    for (batchId <- batchIds) yield {
      JobLogger.log("Started Fetching the Incorrect Max Score Value for the Batch", Option(Map("batch_id" -> batchId, "isDryRunMode" -> isDryRunMode)), INFO)
      process(correctRawAssessment, batchId = batchId, isDryRunMode = isDryRunMode)
    }
  }

  def process(isRawDataCorrection: Boolean, batchId: String, isDryRunMode: Boolean)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig, sc: SparkContext) = {
    if (isRawDataCorrection) {
      updateAssessmentAgg(batchId = batchId, isDryRunMode = isDryRunMode)
    } else {
      updateUserActivityAgg(batchId = batchId, isDryRunMode = isDryRunMode)
    }
  }

  def removeAssessmentRecords(filteredAssessmentData: DataFrame, batchId: String, isDryRunMode: Boolean)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig, sc: SparkContext) = {
    val totalRecords = filteredAssessmentData.count()
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]())
    val outputPath = modelParams.getOrElse("csvPath", "").asInstanceOf[String]
    JobLogger.log("Total Incorrect Records", Option(Map("total_records" -> totalRecords)), INFO)
    if (isDryRunMode) {
      filteredAssessmentData.select("course_id", "batch_id", "content_id", "attempt_id", "user_id", "total_max_score", "total_score")
        .repartition(1).write.option("header", true).format("com.databricks.spark.csv").save(outputPath.concat(s"/assessment-corrected-report-$batchId-${System.currentTimeMillis()}.csv"))
      JobLogger.log("Generated the CSV File", Option(Map("batch_id" -> batchId, "total_records" -> totalRecords), INFO))
    } else {
      JobLogger.log("Deleting the records from the table", Option(Map("total_records" -> totalRecords)), INFO)
      filteredAssessmentData.select("course_id", "batch_id", "user_id", "content_id", "attempt_id").rdd.deleteFromCassandra(AppConf.getConfig("sunbird.courses.keyspace"), "assessment_aggregator")
    }
  }

  def updateAssessmentAgg(batchId: String, isDryRunMode: Boolean)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig, sc: SparkContext): List[Map[String, Any]] = {
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]())
    // Get the Assessment Data for the specific Batch
    val assessmentData: DataFrame = getAssessmentAggData(batchId).select("course_id", "batch_id", "content_id", "attempt_id", "user_id", "total_max_score", "total_score").persist()
    // Take the contentId's which is associated to the batch being invoked for the correction
    val contentIds: List[String] = assessmentData.select("content_id").distinct().collect().map(_ (0)).toList.asInstanceOf[List[String]]
    for (contentId <- contentIds) yield {
      val contentMetaURL: String = modelParams.getOrElse("contentReadAPI", "https://diksha.gov.in/api/content/v1/read/").asInstanceOf[String]
      val supportedContentType: String = modelParams.getOrElse("supportedContentType", "SelfAssess").asInstanceOf[String]
      // Get the TotalQuestion Value from the Content Meta API
      val contentMeta: Map[String, Any] = Await.result[Map[String, Any]](getTotalQuestions(contentId, contentMetaURL), 60.seconds)
      val contentType: String = contentMeta.getOrElse("contentType", "").asInstanceOf[String]
      val totalQuestions: Int = contentMeta.getOrElse("totalQuestions", 0).asInstanceOf[Int]
      JobLogger.log("Fetched the content meta value to the processing batch", Option(contentMeta ++ Map("totalQuestions" -> totalQuestions)), INFO)
      // Filter only supported content Type ie SelfAssess Content Type
      if (StringUtils.equals(contentType, supportedContentType)) {
        val filteredDF = assessmentData.filter(col("content_id") === contentId)
          .filter(col("total_max_score") =!= totalQuestions)
          .select("course_id", "batch_id", "content_id", "attempt_id", "user_id", "total_max_score", "total_score")
        removeAssessmentRecords(filteredDF, batchId, isDryRunMode)
        Map("batch_id" -> batchId, "total_records" -> filteredDF.count(), "content_id" -> contentId, "total_questions" -> totalQuestions)
      } else {
        JobLogger.log("The content ID is not self assess, Skipping data removal", Some(Map("contentId" -> contentId, "contentType" -> contentType)), INFO)
        Map[String, String]()
      }
    }
  }

  // Fetch the assessment data for a specific batch identifier
  def getAssessmentAggData(batchId: String)(implicit spark: SparkSession): DataFrame = {
    fetchData(spark, assessmentAggDBSettings, cassandraFormat, new StructType())
      .filter(col("batch_id") === batchId)
  }

  def getUserActivityAggData(context_id: String)(implicit spark: SparkSession): DataFrame = {
    fetchData(spark, userActivityAggDBSettings, cassandraFormat, new StructType())
      .filter(col("context_id") === context_id)
  }

  def getTotalQuestions(contentId: String, apiUrl: String): Future[Map[String, Any]] = {
    Future {
      val response = RestUtil.get[ContentResponse](apiUrl.concat(contentId))
      if (null != response && response.responseCode.equalsIgnoreCase("ok") && null != response.result.content && response.result.content.nonEmpty) {
        val totalQuestions: Int = response.result.content.getOrElse("totalQuestions", 0).asInstanceOf[Int]
        val contentType: String = response.result.content.getOrElse("contentType", null).asInstanceOf[String]
        Map("totalQuestions" -> totalQuestions, "contentType" -> contentType, "contentId" -> contentId)
      } else {
        Map()
      }
    }
  }

  def updateUserActivityAgg(isDryRunMode: Boolean, batchId: String)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig, sc: SparkContext): List[Map[String, Any]] = {
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]())
    val outputPath = modelParams.getOrElse("csvPath", "").asInstanceOf[String]
    val correctedData = computeScoreMetrics(batchId = batchId)
    val metrics = Map("batch_id" -> batchId, "total_records" -> correctedData.count())
    if (isDryRunMode) {
      // Stringifying the agg and agg_last_updated_on col since CSV Doesn't support the Map object
      correctedData.withColumn("agg", to_json(col("agg"))).withColumn("agg_last_updated", to_json(col("agg_last_updated")))
        .repartition(1).write.option("header",  true).format("com.databricks.spark.csv").save(outputPath.concat(s"/user-activity-agg-corrected-report-$batchId-${System.currentTimeMillis()}.csv"))
      JobLogger.log("Generated a CSV file", Option(metrics), INFO)
    } else {
      correctedData.write.format("org.apache.spark.sql.cassandra").options(userActivityAggDBSettings ++ Map("confirm.truncate" -> "false")).mode(SaveMode.Append).save()
      JobLogger.log("Updated the table", Option(metrics), INFO)
    }
    List(metrics)
  }

  def computeScoreMetrics(batchId: String)(implicit spark: SparkSession): DataFrame = {
    // UDF Methods Initialization
    val updateAggColumn = udf(mergeAggMapCol())
    val updatedAggLastUpdatedCol = udf(mergeAggLastUpdatedMapCol())
    // Get the Best score assessment data for a given batchId and process it
    val assessmentData = getAssessmentAggData(batchId)
    val bestScoreDF: DataFrame = getBestScore(assessmentData)
      .withColumn("context_id", concat(lit("cb:"), col("batch_id")))
      .withColumnRenamed("course_id", "activity_id")

    val activityAggDF: DataFrame = getUserActivityAggData(context_id = "cb:".concat(batchId))

    val resultDF = bestScoreDF.join(activityAggDF, Seq("context_id", "user_id", "activity_id"), "inner")

    // Compute tha agg columns using UDF methods
    resultDF
      .withColumn("agg", updateAggColumn(col("agg").cast("map<string, int>"), col("total_score").cast(sql.types.IntegerType), col("content_id").cast(sql.types.StringType)))
      .withColumn("agg_last_updated", updatedAggLastUpdatedCol(col("agg_last_updated").cast("map<string, long>"), col("content_id").cast(sql.types.StringType)))
      .select("user_id", "context_id", "activity_id", "agg", "agg_last_updated")
  }

  def mergeAggMapCol(): (Map[String, Int], Int, String) => Map[String, Int] = (agg: Map[String, Int], score: Int, content_id: String) => {
    agg ++ Map(s"score:$content_id" -> score)
  }

  def mergeAggLastUpdatedMapCol(): (Map[String, Long], String) => Map[String, Long] = (aggLastUpdated: Map[String, Long], content_id: String) => {
    import java.util.Date
    aggLastUpdated.map(x => Map(x._1 -> new Date(x._2 * 1000).getTime)).flatten.toMap ++ Map(s"score:$content_id" -> System.currentTimeMillis())
  }

  def getBestScore(assessmentData: DataFrame): DataFrame = {
    val df = Window.partitionBy("user_id", "batch_id", "course_id", "content_id").orderBy(desc("total_score"))
    assessmentData.withColumn("rownum", row_number.over(df)).where(col("rownum") === 1).drop("rownum")
  }

}
