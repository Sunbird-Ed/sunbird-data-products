package org.sunbird.analytics.exhaust.util


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.JobLogger

object ExhaustUtil {

  def getAssessmentBlobData(store: String, filePath: String, bucket: String, batchId: Option[String], fileFormat: Option[String])(implicit spark: SparkSession, fc: FrameworkContext): DataFrame = {
    val format = fileFormat.getOrElse("csv")
    val batchid = batchId.getOrElse("")

    val url = store match {
      case "local" =>
        filePath + s"${batchid}-*.${format}"
      // $COVERAGE-OFF$ for azure testing
      case "s3" | "azure" =>
        val key = AppConf.getConfig("azure_storage_key")
        val file = s"${filePath}${batchid}-*.${format}"
        s"wasb://$bucket@$key.blob.core.windows.net/$file."
      // $COVERAGE-ON$
    }

    JobLogger.log(s"Fetching data from ${store} for batchid: " + batchid)(new String())

    spark.read.format("csv")
      .option("header","true")
      .load(url)
  }
}
