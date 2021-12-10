package org.sunbird.analytics.archival

import java.util.concurrent.atomic.AtomicInteger

import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.types.StructType
import org.ekstep.analytics.framework.Level.ERROR
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig, Level}
import org.sunbird.analytics.exhaust.BaseReportsJob
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import org.sunbird.analytics.archival.util.{ArchivalMetaDataStoreJob, ArchivalRequest}

case class Period(year: Int, weekOfYear: Int)

case class BatchPartition(batchId: String, period: Period)
case class Request(archivalTable: String, keyspace: Option[String], query: Option[String] = Option(""), batchId: Option[String] = Option(""), collectionId: Option[String]=Option(""), date: Option[String] = Option(""))
case class ArchivalMetrics(batchId: Option[String],
                           period: Period,
                           totalArchivedRecords: Option[Long],
                           pendingWeeksOfYears: Option[Long],
                           totalDeletedRecords: Option[Long],
                           totalDistinctBatches: Long
                          )

trait BaseArchivalJob extends BaseReportsJob with IJob with ArchivalMetaDataStoreJob with Serializable {

  val cassandraUrl = "org.apache.spark.sql.cassandra"
  def dateColumn: String

  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None): Unit = {
    implicit val className: String = getClassName;
    JobLogger.init(jobName)
    JobLogger.start(s"$jobName started executing - ver3", Option(Map("config" -> config, "model" -> jobName)))
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](config)
    implicit val spark: SparkSession = openSparkSession(jobConfig)
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()

    try {
      val res = CommonUtil.time(execute());
      JobLogger.end(s"$jobName completed execution", "SUCCESS", None)
    } catch {
      case ex: Exception => ex.printStackTrace()
        JobLogger.log(ex.getMessage, None, ERROR);
        JobLogger.end(jobName + " execution failed", "FAILED", Option(Map("model" -> jobName, "statusMsg" -> ex.getMessage)));
    }
    finally {
      frameworkContext.closeContext();
      spark.close()
    }


  }

  def init()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): Unit = {
    spark.setCassandraConf("LMSCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.courses.cluster.host")))
  }

//  def dataFilter(): Unit = {}

  def getClassName: String;

  def execute()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): Unit = {
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]());
    val requestConfig = JSONUtils.deserialize[Request](JSONUtils.serialize(modelParams.getOrElse("request", Request).asInstanceOf[Map[String,AnyRef]]))

    val mode: String = modelParams.getOrElse("mode","archive").asInstanceOf[String]

    val archivalRequests = mode.toLowerCase() match {
      case "archival" =>
        archiveData(requestConfig)
      case "delete" =>
        deleteArchivedData(requestConfig)
    }

    for (archivalRequest <- archivalRequests) {
      upsertRequest(archivalRequest)
    }
  }

  def archivalFormat(batch: BatchPartition): String = {
    s"${batch.batchId}/${batch.period.year}-${batch.period.weekOfYear}"
  }

  def archiveData(requestConfig: Request)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): List[ArchivalRequest]

  def archiveBatches(batchesToArchive: Map[String, Array[BatchPartition]], data: DataFrame, requestConfig: Request)(implicit config: JobConfig): List[ArchivalRequest]

  def deleteArchivedData(archivalRequest: Request): List[ArchivalRequest]

  def processArchival(archivalTableData: DataFrame, archiveRequest: Request)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame;

  def getWeekAndYearVal(date: String): Period = {
    if (null != date && date.nonEmpty) {
      val dt = new DateTime(date)
      Period(year = dt.getYear, weekOfYear = dt.getWeekOfWeekyear)
    } else {
      Period(0, 0)
    }
  }

  def upload(archivedData: DataFrame, batch: BatchPartition)(implicit jobConfig: JobConfig): List[String] = {
    val blobConfig = jobConfig.modelParams.get("blobConfig").asInstanceOf[Map[String, AnyRef]]
    val reportPath: String = blobConfig.getOrElse("reportPath", "archived-data/").asInstanceOf[String]
    val container = AppConf.getConfig("cloud.container.reports")
    val objectKey = AppConf.getConfig("course.metrics.cloud.objectKey")
    val fileName = archivalFormat(batch)
    val storageConfig = getStorageConfig(jobConfig, objectKey)
    JobLogger.log(s"Uploading reports to blob storage", None, Level.INFO)
    archivedData.saveToBlobStore(storageConfig, "csv", s"$reportPath$fileName-${System.currentTimeMillis()}", Option(Map("header" -> "true", "codec" -> "org.apache.hadoop.io.compress.GzipCodec")), None, fileExt=Some("csv.gz"))
  }

  def jobId: String;
  def jobName: String;
  def getReportPath: String;
  def getReportKey: String;

}
