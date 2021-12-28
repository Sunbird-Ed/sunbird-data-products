package org.sunbird.analytics.archival

import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions.udf
import org.ekstep.analytics.framework.Level.ERROR
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig, Level}
import org.sunbird.analytics.exhaust.BaseReportsJob
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.sunbird.analytics.archival.util.{ArchivalMetaDataStoreJob, ArchivalRequest}

case class Request(archivalTable: String, keyspace: Option[String], query: Option[Map[String, AnyRef]] = Option(Map()), batchId: Option[String] = Option(""), collectionId: Option[String]=Option(""), batchFilters: Option[List[String]]=Option(List()), date: Option[String] = Option(""))

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

  def execute()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): Unit = {
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]());
    val requestConfig = JSONUtils.deserialize[Request](JSONUtils.serialize(modelParams.getOrElse("request", Request).asInstanceOf[Map[String,AnyRef]]))
    val mode: String = modelParams.getOrElse("mode","archive").asInstanceOf[String]

    val requests = getRequests(jobId, requestConfig.batchId)

    val archivalRequests = mode.toLowerCase() match {
      case "archival" =>
        archiveData(requestConfig, requests)
      case "delete" =>
        deleteArchivedData(requestConfig, requests)
    }
    for (archivalRequest <- archivalRequests) {
      upsertRequest(archivalRequest)
    }
  }

  def upload(archivedData: DataFrame, batch: Map[String,AnyRef])(implicit jobConfig: JobConfig): List[String] = {
    val blobConfig = jobConfig.modelParams.get("blobConfig").asInstanceOf[Map[String, AnyRef]]
    val reportPath: String = blobConfig.getOrElse("reportPath", "archived-data/").asInstanceOf[String]
    val objectKey = AppConf.getConfig("course.metrics.cloud.objectKey")
    val fileName = archivalFormat(batch)
    val storageConfig = getStorageConfig(jobConfig, objectKey)
    JobLogger.log(s"Uploading reports to blob storage", None, Level.INFO)
    archivedData.saveToBlobStore(storageConfig, "csv", s"$reportPath$fileName-${System.currentTimeMillis()}", Option(Map("header" -> "true", "codec" -> "org.apache.hadoop.io.compress.GzipCodec")), None, fileExt=Some("csv.gz"))
  }

  // Overriding methods START:
  def jobId: String;
  def jobName: String;
  def getReportPath: String;
  def getReportKey: String;
  def getClassName: String;

  def archiveData(requestConfig: Request, requests: Array[ArchivalRequest])(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): List[ArchivalRequest];
  def deleteArchivedData(archivalRequest: Request, requests: Array[ArchivalRequest])(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): List[ArchivalRequest];
  def archivalFormat(batch: Map[String,AnyRef]): String;
  def dataFilter(requests: Array[ArchivalRequest], dataDF: DataFrame): DataFrame;

  //Overriding methods END:

  val tsToLongUdf = udf[java.lang.Long, java.sql.Timestamp]((ts: java.sql.Timestamp) => if (ts != null) ts.getTime else null)
}
