package org.sunbird.analytics.exhaust.util

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{CommonUtil, JobLogger}

object ExhaustUtil {

  def getArchivedData(store: String, filePath: String, bucket: String, blobFields: Map[String, Any], fileFormat: Option[String])(implicit spark: SparkSession, fc: FrameworkContext): DataFrame = {
    val filteredBlobFields = blobFields.filter(_._2 != null)
    val format = fileFormat.getOrElse("csv.gz")
    val batchId = filteredBlobFields.getOrElse("batchId", "*")
    val collectionId = filteredBlobFields.getOrElse("collectionId", "*")
    val year = filteredBlobFields.getOrElse("year", "*")
    val weekNum = filteredBlobFields.getOrElse("weekNum", "*")

    val file: String = s"${filePath}${batchId}_${collectionId}/${year}-${weekNum}-*.${format}"
    val url = CommonUtil.getBlobUrl(store, file, bucket)

    JobLogger.log(s"Fetching data from ${store} ")(new String())
    fetch(url, "csv")
  }

  def fetch(url: String, format: String)(implicit spark: SparkSession, fc: FrameworkContext): DataFrame = {
    spark.read.format(format).option("header", "true").load(url)
  }
}