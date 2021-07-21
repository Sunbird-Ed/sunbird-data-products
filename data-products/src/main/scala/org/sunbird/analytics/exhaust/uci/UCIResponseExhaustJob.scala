package org.sunbird.analytics.exhaust.uci

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.conf.AppConf
import org.sunbird.analytics.exhaust.collection.UDFUtils

object UCIResponseExhaustJob extends optional.Application with BaseUCIExhaustJob {

  override def getClassName = "org.sunbird.analytics.exhaust.collection.ResponseExhaustJob"
  override def jobName() = "UCIResponseExhaustJob";
  override def jobId() = "uci-response-exhaust";
  override def getReportPath() = "uci-response-exhaust/";
  override def getReportKey() = "response";

  override def process(conversationId: String, telemetryDF: DataFrame, conversationDF: DataFrame)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {

    telemetryDF.show(false)
    telemetryDF
      .select(telemetryDF.col("edata"), telemetryDF.col("context"))
      .withColumn("deviceid",explode_outer(col("context.did")))
      .withColumn("questiondata",explode_outer(col("edata.item")))
      .withColumn("questionid" , col("questiondata.id"))
      .withColumn("questiontype", col("questiondata.type"))
      .withColumn("questiontitle", col("questiondata.title"))
      .withColumn("questiondescription", col("questiondata.desc"))
      .withColumn("questionduration", round(col("edata.duration")))
      .withColumn("questionscore", col("edata.score"))
      .withColumn("questionmaxscore", col("questiondata.maxscore"))
      .withColumn("questionresponse", UDFUtils.toJSON(col("edata.resvalues")))
      .withColumn("questionoption", UDFUtils.toJSON(col("questiondata.params")))
      .drop("context", "edata", "questiondata")
  }

}