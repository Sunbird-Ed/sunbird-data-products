package org.sunbird.analytics.exhaust.uci

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.util.JSONUtils
import org.sunbird.analytics.exhaust.collection.UDFUtils

import scala.collection.immutable.List

object UCIResponseExhaustJob extends optional.Application with BaseUCIExhaustJob {

  override def getClassName = "org.sunbird.analytics.exhaust.uci.UCIResponseExhaustJob"
  override def jobName() = "UCIResponseExhaustJob";
  override def jobId() = "uci-response-exhaust";
  override def getReportPath() = "uci-response-exhaust/";
  override def getReportKey() = "response";
  override def zipEnabled(): Boolean = false;

  private val columnsOrder = List("Message ID", "Conversation ID", "Conversation Name", "Device ID", "Question Id", "Question Type",
    "Question Title", "Question Description", "Question Duration", "Question Score", "Question Max Score",
    "Question Options", "Question Response", "X Path", "Eof", "Timestamp")
  private val columnMapping = Map("mid" -> "Message ID", "conversation_id" -> "Conversation ID", "conversation_name" -> "Conversation Name",
    "device_id" -> "Device ID", "question_id" -> "Question Id", "question_type" -> "Question Type", "question_title" -> "Question Title",
    "question_description" -> "Question Description", "question_duration" -> "Question Duration", "question_score" -> "Question Score", "question_maxscore" -> "Question Max Score",
    "question_response" -> "Question Response", "question_option" -> "Question Options", "x_path" -> "X Path", "eof" -> "Eof", "timestamp" -> "Timestamp")

  override def process(conversationId: String, telemetryDF: DataFrame, conversationDF: DataFrame)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {

    if (telemetryDF.count() > 0) {
      val conversationName = conversationDF.head().getAs[String]("name")
      val userDF = loadUserTable()
      val finalDF = telemetryDF
        .select(telemetryDF.col("edata"), telemetryDF.col("context"), telemetryDF.col("context.rollup"), telemetryDF.col("mid"), telemetryDF.col("@timestamp"))
        .withColumn("conversation_id", lit(conversationId))
        .withColumn("conversation_name", lit(conversationName))
        .withColumn("device_id",col("context.did"))
        .withColumn("question_id" , col("edata.item.id"))
        .withColumn("question_type", col("edata.item.type"))
        .withColumn("question_title", col("edata.item.title"))
        .withColumn("question_description", col("edata.item.desc"))
        .withColumn("question_duration", round(col("edata.duration")))
        .withColumn("question_score", col("edata.score"))
        .withColumn("question_maxscore", col("edata.item.maxscore"))
        .withColumn("question_response", to_json(col("edata.resvalues")))
        .withColumn("question_option", to_json(col("edata.item.params")))
        .withColumn("mid", col("mid"))
        .withColumn("x_path", col("rollup.l3"))
        .withColumn("eof", col("rollup.l4"))
        .withColumn("timestamp", col("@timestamp"))
        .join(userDF, Seq("device_id"), "inner")
        .withColumn("question_response", when(col("consent") === true, col("question_response")).otherwise(lit("")))
        .drop("context", "edata", "data", "consent", "rollup")
      organizeDF(finalDF, columnMapping, columnsOrder)
    }
    else spark.emptyDataFrame
  }

  def getConsentValueFn: String => Boolean = (device_data: String) => {
    val device = JSONUtils.deserialize[Map[String, AnyRef]](device_data)
    device.getOrElse("device", Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("consent", isConsentToShare).asInstanceOf[Boolean]
  }

  /**
   * Fetch the user table data to get the consent information
   */
  def loadUserTable()(implicit spark: SparkSession, fc: FrameworkContext): DataFrame = {
    val consentValue = spark.udf.register("consent", getConsentValueFn)
    fetchData(fusionAuthURL, fushionAuthconnectionProps, userTable).select("id", "data")
      .withColumnRenamed("id", "device_id")
      .withColumn("consent", consentValue(col("data")))
  }
}
