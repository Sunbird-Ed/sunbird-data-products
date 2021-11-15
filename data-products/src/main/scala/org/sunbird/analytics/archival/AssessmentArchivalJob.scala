package org.sunbird.analytics.archival

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}

object AssessmentArchivalJob extends optional.Application with BaseArchivalJob {

  override def getClassName = "org.sunbird.analytics.archival.AssessmentArchivalJob"
  override def jobName() = "AssessmentArchivalJob";
  override def jobId(): String = "assessment-archival";
  override def getReportPath() = "assessment-archival/";
  override def getReportKey() = "assessment";

  override def processArchival(archivalTableData: DataFrame, requestConfig: Request)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {
    println("Process Archival")

    spark.emptyDataFrame
  }

}
