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
import org.sunbird.analytics.archival.util.ArchivalMetaDataStoreJob

case class Period(year: Int, weekOfYear: Int)

case class BatchPartition(batchId: String, period: Period)
case class Request(archivalTable: String, keyspace: Option[String], query: Option[String] = Option(""), batchId: Option[String] = Option(""), collectionId: Option[String]=Option(""), date: Option[String] = Option(""))

trait BaseArchivalJob extends BaseReportsJob with IJob with ArchivalMetaDataStoreJob with Serializable {

  private val partitionCols = List("batch_id", "year", "week_of_year")
  private val columnWithOrder = List("course_id", "batch_id", "user_id", "content_id", "attempt_id", "created_on", "grand_total", "last_attempted_on", "total_max_score", "total_score", "updated_on", "question")
  val cassandraUrl = "org.apache.spark.sql.cassandra"

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
//  def dateFormat(): String;
  def getClassName: String;

  def execute()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): Unit = {
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]());
    val archivalRequest = JSONUtils.deserialize[Request](JSONUtils.serialize(modelParams.getOrElse("request", Request).asInstanceOf[Map[String,AnyRef]]))
    val archivalTable = archivalRequest.archivalTable
    val archivalKeyspace = archivalRequest.keyspace.getOrElse(AppConf.getConfig("sunbird.courses.keyspace"))

    val batchId: String = archivalRequest.batchId.getOrElse("")
    val date: String  = archivalRequest.date.getOrElse("")
    val mode: String = modelParams.getOrElse("mode","archive").asInstanceOf[String]

    println("modelParams: " + modelParams)
    println("archival request: " + archivalRequest)
    val archivalTableData: DataFrame = getArchivalData(archivalTable, archivalKeyspace,Option(batchId),Option(date))
    println("archivalTableData ")
    archivalTableData.show(false)

    mode.toLowerCase() match {
      case "archival" =>
        archiveData(archivalTableData, archivalRequest)
      case "delete" =>
        deleteArchivedData(archivalTableData,archivalRequest)
    }
  }

  def archiveData(data: DataFrame, archivalRequest: Request)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): Unit = {
    val requests = getRequests(jobId, archivalRequest.batchId)
    println("requestLength: " + requests.length)
    try {
      if(requests.length == 0) {
        val groupedDF = data.withColumn("updated_on", to_timestamp(col("updated_on")))
          .withColumn("year", year(col("updated_on")))
          .withColumn("week_of_year", weekofyear(col("updated_on")))
          .withColumn("question", to_json(col("question")))
        groupedDF.show(false)
        val archiveBatchList = groupedDF.groupBy(partitionCols.head, partitionCols.tail: _*).count().collect()
        println("archiveBatchList: " + archiveBatchList.head)

        val batchesToArchive: Map[String, Array[BatchPartition]] = archiveBatchList.map(f => BatchPartition(f.get(0).asInstanceOf[String], Period(f.get(1).asInstanceOf[Int], f.get(2).asInstanceOf[Int]))).groupBy(_.batchId)

        val archivalStatus = batchesToArchive.flatMap(batches => {
          val processingBatch = new AtomicInteger(batches._2.length)
          //          JobLogger.log(s"Started Processing to archive the data", Some(Map("batch_id" -> batches._1, "total_part_files_to_archive" -> batches._2.length)))
          // Loop through the week_num & year batch partition
          val res = for (batch <- batches._2.asInstanceOf[Array[BatchPartition]]) yield {
            val filteredDF = data.filter(col("batch_id") === batch.batchId && col("year") === batch.period.year && col("week_of_year") === batch.period.weekOfYear).select(columnWithOrder.head, columnWithOrder.tail: _*)
            upload(filteredDF, batch) // Upload the archived files into blob store
          }
        })
      }
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
    processArchival(data, archivalRequest)
  }

  def deleteArchivedData(data: DataFrame, archivalRequest: Request): Unit = {

  }

  def processArchival(archivalTableData: DataFrame, archivalRequest: Request)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame;

  def getArchivalData(table: String, keyspace: String, batchId: Option[String], date: Option[String])(implicit spark: SparkSession, fc: FrameworkContext): DataFrame = {
    val archivalTableSettings = Map("table" -> table, "keyspace" -> keyspace, "cluster" -> "LMSCluster")
    val archivalDBDF = loadData(archivalTableSettings, cassandraUrl, new StructType())
    val batchIdentifier = batchId.getOrElse(null)

    if (batchIdentifier.nonEmpty) {
      archivalDBDF.filter(col("batch_id") === batchIdentifier).persist()
    } else {
      archivalDBDF
    }
  }

  def getWeekAndYearVal(date: String): Period = {
    if (null != date && date.nonEmpty) {
      val dt = new DateTime(date)
      Period(year = dt.getYear, weekOfYear = dt.getWeekOfWeekyear)
    } else {
      Period(0, 0)
    }
  }
  def upload(archivedData: DataFrame,
             batch: BatchPartition)(implicit jobConfig: JobConfig): List[String] = {
    val modelParams = jobConfig.modelParams.get
    val reportPath: String = modelParams.getOrElse("reportPath", "archived-data/").asInstanceOf[String]
    val container = AppConf.getConfig("cloud.container.reports")
    val objectKey = AppConf.getConfig("course.metrics.cloud.objectKey")
    val fileName = s"${batch.batchId}/${batch.period.year}-${batch.period.weekOfYear}"
    val storageConfig = getStorageConfig(jobConfig, objectKey)
    JobLogger.log(s"Uploading reports to blob storage", None, Level.INFO)
    archivedData.saveToBlobStore(storageConfig, "csv", s"$reportPath$fileName-${System.currentTimeMillis()}", Option(Map("header" -> "true", "codec" -> "org.apache.hadoop.io.compress.GzipCodec")), None, Some("csv.gz"))
  }

  def jobId: String;
  def jobName: String;
  def getReportPath: String;
  def getReportKey: String;

}
