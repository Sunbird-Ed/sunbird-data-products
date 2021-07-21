package org.sunbird.analytics.exhaust.uci

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.conf.AppConf
import org.sunbird.analytics.exhaust.collection.UDFUtils

import scala.collection.immutable.List

object UCIResponseExhaustJob extends optional.Application with BaseUCIExhaustJob {

  override def getClassName = "org.sunbird.analytics.exhaust.uci.UCIResponseExhaustJob"
  override def jobName() = "UCIResponseExhaustJob";
  override def jobId() = "uci-response-exhaust";
  override def getReportPath() = "uci-response-exhaust/";
  override def getReportKey() = "response";
  override def zipEnabled(): Boolean = false;

  private val columnsOrder = List("Conversation ID", "Conversation Name", "Device ID", "Question Id", "Question Type",
    "Question Title", "Question Description", "Question Duration", "Question Score", "Question Max Score",
    "Question Options", "Question Response")
  private val columnMapping = Map("conversationId" -> "Conversation ID", "conversationName" -> "Conversation Name",
    "deviceid" -> "Device ID", "questionid" -> "Question Id", "questiontype" -> "Question Type", "questiontitle" -> "Question Title",
    "questiondescription" -> "Question Description", "questionduration" -> "Question Duration", "questionscore" -> "Question Score", "questionmaxscore" -> "Question Max Score",
    "questionresponse" -> "Question Response", "questionoption" -> "Question Options")

  override def process(conversationId: String, telemetryDF: DataFrame, conversationDF: DataFrame)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {

    val conversationName = conversationDF.head().getAs[String]("name")
    val finalDF = telemetryDF
      .select(telemetryDF.col("edata"), telemetryDF.col("context"))
      .withColumn("conversationId", lit(conversationId))
      .withColumn("conversationName", lit(conversationName))
      .withColumn("deviceid",col("context.did"))
      .withColumn("questionid" , col("edata.item.id"))
      .withColumn("questiontype", col("edata.item.type"))
      .withColumn("questiontitle", col("edata.item.title"))
      .withColumn("questiondescription", col("edata.item.desc"))
      .withColumn("questionduration", round(col("edata.duration")))
      .withColumn("questionscore", col("edata.score"))
      .withColumn("questionmaxscore", col("edata.item.maxscore"))
      .withColumn("questionresponse", UDFUtils.toJSON(col("edata.resvalues")))
      .withColumn("questionoption", UDFUtils.toJSON(col("edata.item.params")))
      .drop("context", "edata")
    organizeDF(finalDF, columnMapping, columnsOrder)
  }

}