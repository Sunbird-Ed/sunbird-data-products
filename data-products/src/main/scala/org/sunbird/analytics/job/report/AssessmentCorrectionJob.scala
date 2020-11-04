package org.sunbird.analytics.job.report

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobDriver}
import org.sunbird.analytics.model.report.AssessmentCorrectionModel

object AssessmentCorrectionJob extends optional.Application with IJob {

  implicit val className = "org.ekstep.analytics.job.AssessmentCorrectionJob"

  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
    implicit val sparkContext: SparkContext = sc.getOrElse(null);
    JobLogger.log("Started executing Job")
    JobDriver.run("batch", config, AssessmentCorrectionModel);
    JobLogger.log("Job Completed.")
  }

}
