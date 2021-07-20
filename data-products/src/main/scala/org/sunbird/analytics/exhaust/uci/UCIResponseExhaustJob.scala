package org.sunbird.analytics.exhaust.uci

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.conf.AppConf

object UCIResponseExhaustJob extends optional.Application with BaseUCIExhaustJob {
  
  override def getClassName = "org.sunbird.analytics.exhaust.collection.ResponseExhaustJob"
  override def jobName() = "UCIResponseExhaustJob";
  override def jobId() = "uci-response-exhaust";
  override def getReportPath() = "uci-response-exhaust/";
  override def getReportKey() = "response";

  override def process(conversationId: String, telemetryDF: DataFrame)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {
    telemetryDF
  }

}