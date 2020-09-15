package org.sunbird.analytics.exhaust.collection

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.JobConfig

object ResponseExhaustJob extends optional.Application with BaseCollectionExhaustJob {
  
  override def getClassName = "org.sunbird.analytics.exhaust.collection.ResponseExhaustJob"
  override def jobName() = "ResponseExhaustJob";
  override def jobId() = "response-exhaust";
  override def getReportPath() = "response-exhaust/";
  override def getReportKey() = "response";

  override def processBatch(userEnrolmentDF: DataFrame, collectionBatch: CollectionBatch)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {
    userEnrolmentDF;
  }
  
}