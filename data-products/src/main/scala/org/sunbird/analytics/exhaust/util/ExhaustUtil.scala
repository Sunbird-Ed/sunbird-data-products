package org.sunbird.analytics.exhaust.util


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.JobLogger

object ExhaustUtil {

  def getArchivedData(store: String, filePath: String, bucket: String, blobFields: Map[String, String], fileFormat: Option[String])(implicit spark: SparkSession, fc: FrameworkContext): DataFrame = {
    val filteredBlobFields = blobFields.filter(_._2 != null)
    val format = fileFormat.getOrElse("csv.gz")
    val batchId = filteredBlobFields.getOrElse("batchId", "*")
    val year = filteredBlobFields.getOrElse("year", "*")
    val weekNumb = filteredBlobFields.getOrElse("weekNum", "*")


    val url = store match {
      case "local" =>
        filePath + s"${batchId}/${year}-${weekNumb}-*.${format}"
      // $COVERAGE-OFF$ for azure testing
      case "s3" | "azure" =>
        val key = AppConf.getConfig("azure_storage_key")
        val file = s"${filePath}${batchId}/${year}-${weekNumb}-*.${format}"
        s"wasb://$bucket@$key.blob.core.windows.net/$file."
      // $COVERAGE-ON$
    }

    JobLogger.log(s"Fetching data from ${store} ")(new String())
    fetch(url, "csv")
  }

  def fetch(url: String, format: String)(implicit spark: SparkSession, fc: FrameworkContext): DataFrame = {
    spark.read.format(format).option("header", "true").load(url)
  }

}