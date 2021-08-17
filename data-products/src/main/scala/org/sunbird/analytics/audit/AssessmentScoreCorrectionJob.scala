package org.sunbird.analytics.audit

import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
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

  // $COVERAGE-ON$
  def processBatches()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig, sc: SparkContext): List[Map[String, Any]] = {
    val batchIds: List[String] = AppConf.getConfig("assessment.score.correction.batches").split(",").toList.filter(x => x.nonEmpty)
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]())
    val isDryRunMode = modelParams.getOrElse("isDryRunMode", true).asInstanceOf[Boolean]
    for (batchId <- batchIds) yield {
      JobLogger.log("Started Correcting the Max Score Value for the Batch", Option(Map("batch_id" -> batchId, "isDryRunMode" -> isDryRunMode)), INFO)
      correctRecords(batchId = batchId, isDryRunMode = isDryRunMode)
    }
  }

  def correctRecords(batchId: String, isDryRunMode: Boolean)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig, sc: SparkContext) = {
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]())
    val outputPath = modelParams.getOrElse("csvPath", "").asInstanceOf[String]

    // Register the UDF Methods
    val create_score_map = spark.udf.register("createMaxScoreMap", createScoreMetricsMap) 
    val compute_score_metrics = spark.udf.register("computeMaxScore", computeScoreMetrics)

    // Get the Assessment Data for the specific Batch
    val assessmentData: DataFrame = getAssessmentAggData(batchId).select("course_id", "batch_id", "content_id", "attempt_id", "user_id", "total_max_score", "question")

    // Take the content Id which is associated to the batch being invoked for the correction
    val contentId: String = assessmentData.select("content_id").collect().map(_ (0)).toList.head.asInstanceOf[String]

    val contentMetaURL: String = modelParams.getOrElse("contentReadAPI", "https://diksha.gov.in/api/content/v1/read/").asInstanceOf[String]
    // Get the TotalQuestion Value from the Content Meta API
    val totalQuestions: Int = Await.result[Int](getTotalQuestions(contentId, contentMetaURL), 60.seconds)
    JobLogger.log("Fetched the total questions value for the processing batch", Option(Map("batch_id" -> batchId, "content_id" -> contentId, "total_questions" -> totalQuestions)), INFO)

    // Filter the Assessment Where max_score != totalQuestions Value
    val filteredAssessmentData: DataFrame = assessmentData.filter(col("total_max_score") =!= totalQuestions)
      .withColumn("questionData", explode_outer(col("question")))
      .withColumn("question_ts", col("questionData.assess_ts"))
      .withColumn("question_max_score", col("questionData.max_score"))
      .withColumn("question_score", col("questionData.score"))
      .withColumn("question_score_metrics_map", create_score_map(col("question_max_score"), col("question_score"), col("question_ts")))
      .drop("questionData", "question")

    // Apply sort logic and compute the max_score from the question data column from the UDF method
    val result = filteredAssessmentData
      .groupBy("batch_id", "course_id", "user_id", "attempt_id", "content_id")
      .agg(collect_list("question_score_metrics_map").as("question_score_metrics_map"))
      .withColumn("computed_score_metrics", compute_score_metrics(col("question_score_metrics_map"), lit(totalQuestions)))
      .select(col("batch_id"), col("course_id"), col("user_id"), col("attempt_id"), col("content_id"),
        col("computed_score_metrics").getItem("total_score").as("total_score"),
        col("computed_score_metrics").getItem("total_max_score").as("total_max_score")
      ).withColumn("grand_total", concat_ws("/", col("total_score"), col("total_max_score")))

    val total_records = result.count()
    JobLogger.log("Computed the max_score for all the records", Option(Map("batch_id" -> batchId, "total_records" -> total_records)), INFO)

    if (isDryRunMode) {
      result.repartition(1).write.format("com.databricks.spark.csv").save(outputPath)
      JobLogger.log("Generated a CSV file", Option(Map("batch_id" -> batchId, "total_records" -> total_records)), INFO)
    } else {
      result.write.format("org.apache.spark.sql.cassandra").options(assessmentAggDBSettings ++ Map("confirm.truncate" -> "false")).mode(SaveMode.Append).save()
      JobLogger.log("Updated the table", Option(Map("batch_id" -> batchId, "total_records" -> total_records)), INFO)
    }
    Map("batch_id" -> batchId, "total_records" -> total_records, "content_meta_total_question" -> totalQuestions)
  }

  def createScoreMetricsMap = (max_score: Int, score: Int, assess_ts: String) => {
    Map("max_score" -> max_score.toString, "score" -> score.toString, "assess_ts" -> assess_ts)
  }

  def computeScoreMetrics = (listObj: Seq[Map[String, String]], contentMetaMaxScore: Int) => {
    var totalMaxScore = 0
    var totalScore = 0
    // Sorting the max_score by assess_ts and taking only contentMetaMaxScore Records and aggregate the max_score value
    val sortedQuestions = listObj.sortBy(_ ("assess_ts"))(Ordering[String].reverse).take(contentMetaMaxScore)
    sortedQuestions.foreach(event => {
      totalMaxScore = totalMaxScore + event("max_score").toInt
      totalScore = totalScore + event("score").toInt
    })
    Map("total_score" -> totalScore, "total_max_score" -> totalMaxScore)
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
