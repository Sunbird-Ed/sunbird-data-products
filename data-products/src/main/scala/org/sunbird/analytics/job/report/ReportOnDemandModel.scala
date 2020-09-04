package org.sunbird.analytics.job.report

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.FrameworkContext

trait ReportOnDemandModel {

  def execute(reportParams: Option[Map[String, AnyRef]])(implicit spark: SparkSession, fc: FrameworkContext): Unit = ???

}
