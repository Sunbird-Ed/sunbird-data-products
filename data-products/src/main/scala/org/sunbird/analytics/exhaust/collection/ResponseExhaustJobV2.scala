package org.sunbird.analytics.exhaust.collection

import org.apache.spark.sql.functions.{col, explode_outer, round}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.sunbird.analytics.exhaust.util.ExhaustUtil

object ResponseExhaustJobV2 extends optional.Application with BaseCollectionExhaustJob {

  override def getClassName = "org.sunbird.analytics.exhaust.collection.ResponseExhaustJobV2"
  override def jobName() = "ResponseExhaustJobV2";
  override def jobId() = "response-exhaust";
  override def getReportPath() = "response-exhaust/";
  override def getReportKey() = "response";

  private val persistedDF:scala.collection.mutable.ListBuffer[DataFrame] = scala.collection.mutable.ListBuffer[DataFrame]();

  private val assessmentAggDBSettings = Map("table" -> "assessment_aggregator", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster");

  private val filterColumns = Seq("courseid", "collectionName", "batchid", "batchName", "userid",  "content_id", "contentname", "attempt_id", "last_attempted_on", "questionid",
    "questiontype", "questiontitle", "questiondescription", "questionduration", "questionscore", "questionmaxscore", "questionoption", "questionresponse");
  private val columnsOrder = List("Collection Id", "Collection Name", "Batch Id", "Batch Name", "User UUID", "QuestionSet Id", "QuestionSet Title", "Attempt Id", "Attempted On",
    "Question Id", "Question Type", "Question Title", "Question Description", "Question Duration", "Question Score", "Question Max Score", "Question Options", "Question Response");
  val columnMapping = Map("courseid" -> "Collection Id", "collectionName" -> "Collection Name", "batchid" -> "Batch Id", "batchName" -> "Batch Name", "userid" -> "User UUID",
    "content_id" -> "QuestionSet Id", "contentname" -> "QuestionSet Title", "attempt_id" -> "Attempt Id", "last_attempted_on" -> "Attempted On", "questionid" -> "Question Id",
    "questiontype" -> "Question Type", "questiontitle" -> "Question Title", "questiondescription" -> "Question Description", "questionduration" -> "Question Duration",
    "questionscore" -> "Question Score", "questionmaxscore" -> "Question Max Score", "questionoption" -> "Question Options", "questionresponse" -> "Question Response")

  case class AssessmentData(user_id: String, course_id: String,batch_id: String,content_id: String, attempt_id: String,created_on: Option[String],grand_total: Option[String],last_attempted_on: Option[String],question: List[Question],total_max_score: Option[Double],total_score: Option[Double],updated_on: Option[String]) extends scala.Product with scala.Serializable
  case class Question(id: String, assess_ts: String,max_score: Double, score: Double,`type`: String,title: String,resvalues: List[Map[String, String]],params: List[Map[String, String]],description: String,duration: Double) extends scala.Product with scala.Serializable

  override def processBatch(userEnrolmentDF: DataFrame, collectionBatch: CollectionBatch)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {
    val assessmentDF = getAssessmentDF(userEnrolmentDF, collectionBatch).persist();
    persistedDF.append(assessmentDF);
    val contentIds = assessmentDF.select("content_id").dropDuplicates().collect().map(f => f.get(0));
    val contentDF = searchContent(Map("request" -> Map("filters" -> Map("identifier" -> contentIds)))).withColumnRenamed("collectionName", "contentname").select("identifier", "contentname");
    val reportDF = assessmentDF.join(contentDF, assessmentDF("content_id") === contentDF("identifier"), "left_outer").drop("identifier").select(filterColumns.head, filterColumns.tail: _*);
    organizeDF(reportDF, columnMapping, columnsOrder);
  }

  override def unpersistDFs() {
    persistedDF.foreach(f => f.unpersist(true))
  }

  def getAssessmentDF(userEnrolmentDF: DataFrame, batch: CollectionBatch)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {

    val userEnrolmentDataDF = userEnrolmentDF
      .select(
        col("userid"),
        col("courseid"),
        col("collectionName"),
        col("batchName"),
        col("batchid"))

    val batchid = userEnrolmentDataDF.select("batchid").distinct().collect().head.getString(0)

    val assessAggregateData = loadData(assessmentAggDBSettings, cassandraFormat, new StructType())

    val joinedDF = try {
      val assessBlobData = getAssessmentBlobDF(batchid, config)

      val joinDF = assessAggregateData.join(assessBlobData, Seq("batch_id", "course_id", "user_id"), "left")
          .select(assessAggregateData.col("*"))
      joinDF
    } catch {
      case e => JobLogger.log("Blob does not contain any file for batchid: " + batchid)
        assessAggregateData
    }

    joinedDF.join(userEnrolmentDataDF, joinedDF.col("user_id") === userEnrolmentDataDF.col("userid") && joinedDF.col("course_id") === userEnrolmentDataDF.col("courseid"), "inner")
      .select(userEnrolmentDataDF.col("*"), joinedDF.col("question"), col("content_id"), col("attempt_id"), col("last_attempted_on"))
      .withColumn("questiondata",explode_outer(col("question")))
      .withColumn("questionid" , col("questiondata.id"))
      .withColumn("questiontype", col("questiondata.type"))
      .withColumn("questiontitle", col("questiondata.title"))
      .withColumn("questiondescription", col("questiondata.description"))
      .withColumn("questionduration", round(col("questiondata.duration")))
      .withColumn("questionscore", col("questiondata.score"))
      .withColumn("questionmaxscore", col("questiondata.max_score"))
      .withColumn("questionresponse", UDFUtils.toJSON(col("questiondata.resvalues")))
      .withColumn("questionoption", UDFUtils.toJSON(col("questiondata.params")))
      .drop("question", "questiondata", "question_data")
  }

  def getAssessmentBlobDF(batchid: String, config: JobConfig)(implicit spark: SparkSession, fc: FrameworkContext): DataFrame = {
    val azureFetcherConfig = config.modelParams.get("assessmentFetcherConfig").asInstanceOf[Map[String, AnyRef]]

    val store = azureFetcherConfig("store").asInstanceOf[String]
    val format:String = azureFetcherConfig.getOrElse("format", "csv").asInstanceOf[String]
    val filePath = azureFetcherConfig.getOrElse("filePath", "data-archival/").asInstanceOf[String]
    val container = azureFetcherConfig.getOrElse("container", "reports").asInstanceOf[String]

    val assessAggData = ExhaustUtil.getAssessmentBlobData(store, filePath, container, Option(batchid), Option(format))

    assessAggData.distinct()
      .withColumn("question", UDFUtils.convertStringToList(col("question")))
  }
}
