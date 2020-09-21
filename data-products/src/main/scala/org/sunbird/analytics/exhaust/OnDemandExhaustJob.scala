package org.sunbird.analytics.exhaust

import java.io.File
import java.nio.file.Paths
import java.util.Properties

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.StorageConfig
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.CommonUtil
import org.apache.spark.sql.functions._

import net.lingala.zip4j.ZipFile
import net.lingala.zip4j.model.ZipParameters
import net.lingala.zip4j.model.enums.EncryptionMethod

case class JobRequest(tag: String, request_id: String, job_id: String, var status: String, request_data: String, requested_by: String, requested_channel: String,
                      dt_job_submitted: Long, var download_urls: Option[List[String]], var dt_file_created: Option[Long], var dt_job_completed: Option[Long], 
                      var execution_time: Option[Long], var err_message: Option[String], var iteration: Option[Int], encryption_key: Option[String]) {
    def this() = this("", "", "", "", "", "", "", 0, None, None, None, None, None, None, None)
}

trait OnDemandExhaustJob {

  val connProperties: Properties = CommonUtil.getPostgresConnectionProps()
  val db: String = AppConf.getConfig("postgres.db")
  val url: String = AppConf.getConfig("postgres.url") + s"$db"
  val requestsTable: String = AppConf.getConfig("postgres.table.job_request")
  val jobStatus = List("SUBMITTED","FAILED") 
  val maxIterations = 3;

  def getRequests(jobId: String)(implicit spark: SparkSession, fc: FrameworkContext): Array[JobRequest] = {
    val encoder = Encoders.product[JobRequest]
    import org.apache.spark.sql.functions.col
    val reportConfigsDf = spark.read.jdbc(url, requestsTable, connProperties)
      .where(col("job_id") === jobId && col("iteration") < 3).filter(col("status").isin(jobStatus:_*))
      
    reportConfigsDf.withColumn("status", lit("PROCESSING")).write.jdbc(url, requestsTable, connProperties);
    reportConfigsDf.as[JobRequest](encoder).collect()
  }

  def saveRequests(storageConfig: StorageConfig, requests: Array[JobRequest])(implicit spark: SparkSession, fc: FrameworkContext) = {
    val zippedRequests = for (request <- requests) yield {
      val downloadURLs = for (url <- request.download_urls.get) yield {
        zipAndEncrypt(url, storageConfig, request.encryption_key);
      };
      request.download_urls = Option(downloadURLs);
      request;
    }
    val df = spark.createDataFrame(zippedRequests);
    df.write.jdbc(url, requestsTable, connProperties)
  }

  private def zipAndEncrypt(url: String, storageConfig: StorageConfig, encryptionKey: Option[String])(implicit fc: FrameworkContext): String = {

    val path = Paths.get(url);
    val storageService = fc.getStorageService(storageConfig.store, storageConfig.accountKey.getOrElse(""), storageConfig.secretKey.getOrElse(""));
    val localPath = AppConf.getConfig("spark_output_temp_dir") + path.getFileName;
    val filePrefix = storageConfig.store.toLowerCase() match {
      case "s3" =>
        CommonUtil.getS3File(storageConfig.container, "");
      case "azure" =>
        CommonUtil.getAzureFile(storageConfig.container, "", storageConfig.accountKey.getOrElse("azure_storage_key"))
      case _ =>
        storageConfig.fileName
    }
    val objKey = url.replace(filePrefix, "");
    storageService.download(storageConfig.container, objKey, localPath, Some(false));

    val zipPath = localPath.replace("csv", "zip")
    val zipObjectKey = objKey.replace("csv", "zip")

    encryptionKey.map(key => {
      val zipParameters = new ZipParameters();
      zipParameters.setEncryptFiles(true);
      zipParameters.setEncryptionMethod(EncryptionMethod.ZIP_STANDARD); // AES encryption is not supported by default with various OS.
      val zipFile = new ZipFile(zipPath, key.toCharArray());
      zipFile.addFile(localPath, zipParameters)
    }).getOrElse({
      new ZipFile(zipPath).addFile(new File(localPath));
    })
    storageService.upload(storageConfig.container, zipPath, zipObjectKey, None, Some(0), Some(3), None);
  }
}