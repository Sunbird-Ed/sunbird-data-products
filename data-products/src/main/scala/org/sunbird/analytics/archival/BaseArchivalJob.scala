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
    val requestConfig = JSONUtils.deserialize[Request](JSONUtils.serialize(modelParams.getOrElse("request", Request).asInstanceOf[Map[String,AnyRef]]))
    val archivalTable = requestConfig.archivalTable
    val archivalKeyspace = requestConfig.keyspace.getOrElse(AppConf.getConfig("sunbird.courses.keyspace"))

    val batchId: String = requestConfig.batchId.getOrElse("")
    val date: String  = requestConfig.date.getOrElse("")
    val mode: String = modelParams.getOrElse("mode","archive").asInstanceOf[String]

    println("modelParams: " + modelParams)
    println("archival request: " + requestConfig)
    val archivalTableData: DataFrame = getArchivalData(archivalTable, archivalKeyspace,Option(batchId),Option(date))
    println("archivalTableData ")
    archivalTableData.show(false)

    mode.toLowerCase() match {
      case "archival" =>
        archiveData(archivalTableData, requestConfig)
      case "delete" =>
        deleteArchivedData(archivalTableData,requestConfig)
    }
  }

  def archiveData(data: DataFrame, requestConfig: Request)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): Unit = {
    val requests = getRequests(jobId, requestConfig.batchId)
    println("requestLength: " + requests.length)
    try {
      var dataDF = processArchival(data, requestConfig)
      if(requests.length > 0) {
        for (request <- requests) {
          // TODO: for each request
          if (request.archival_status.equals("SUCCESS")) {
            val request_data = JSONUtils.deserialize[Map[String, AnyRef]](request.request_data)
            dataDF = dataDF.filter(
              col("week_of_year").notEqual(request_data.get("week").get) &&
              col("year").notEqual(request_data.get("year").get)
            )
          }
        }
      }

      val archiveBatchList = dataDF.groupBy(partitionCols.head, partitionCols.tail: _*).count().collect()

      val batchesToArchive: Map[String, Array[BatchPartition]] = archiveBatchList.map(f => BatchPartition(f.get(0).asInstanceOf[String], Period(f.get(1).asInstanceOf[Int], f.get(2).asInstanceOf[Int]))).groupBy(_.batchId)

      archiveBatches(batchesToArchive, dataDF, requestConfig)
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
  }

  def generatePeriodInData(data: DataFrame): DataFrame = {
    data.withColumn("updated_on", to_timestamp(col("updated_on")))
      .withColumn("year", year(col("updated_on")))
      .withColumn("week_of_year", weekofyear(col("updated_on")))
      .withColumn("question", to_json(col("question")))
  }

  def archiveBatches(batchesToArchive: Map[String, Array[BatchPartition]], data: DataFrame, requestConfig: Request)(implicit config: JobConfig): Unit = {
    batchesToArchive.foreach(batches => {
      val processingBatch = new AtomicInteger(batches._2.length)
      JobLogger.log(s"Started Processing to archive the data", Some(Map("batch_id" -> batches._1, "total_part_files_to_archive" -> processingBatch)))

      // Loop through the week_num & year batch partition
      batches._2.map((batch: BatchPartition) => {
        val filteredDF = data.filter(col("batch_id") === batch.batchId && col("year") === batch.period.year && col("week_of_year") === batch.period.weekOfYear).select(columnWithOrder.head, columnWithOrder.tail: _*)
        val collectionId = filteredDF.first().getAs[String]("course_id")
        var archivalRequest = getRequest(collectionId, batch.batchId, batch.period.year, batch.period.weekOfYear)

        if (archivalRequest != null) {
          val request_data = JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(Request)) ++ Map[String, Int](
            "week" -> batch.period.weekOfYear,
            "year"-> batch.period.year
          )
          archivalRequest = ArchivalRequest("", batch.batchId, collectionId, Some(getReportKey), jobId, null, null, null, null, null, Some(0), JSONUtils.serialize(request_data), null)
        }

        try {
          val urls = upload(filteredDF, batch) // Upload the archived files into blob store
          archivalRequest.blob_url = Some(urls)
          JobLogger.log(s"Data is archived and Processing the remaining part files ", None, Level.INFO)
          markRequestAsSuccess(archivalRequest, requestConfig)
        } catch {
          case ex: Exception => {
            markArchivalRequestAsFailed(archivalRequest, ex.getLocalizedMessage)
          }
        }
      }).foreach((archivalRequest: ArchivalRequest) => {
        upsertRequest(archivalRequest)
      })

      JobLogger.log(s"${batches._1} is successfully archived", Some(Map("batch_id" -> batches._1)), Level.INFO)
    })
  }

  def deleteArchivedData(data: DataFrame, archivalRequest: Request): Unit = {

  }

  def processArchival(archivalTableData: DataFrame, archiveRequest: Request)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame;

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
    archivedData.saveToBlobStore(storageConfig, "csv", s"$reportPath$fileName-${System.currentTimeMillis()}", Option(Map("header" -> "true", "codec" -> "org.apache.hadoop.io.compress.GzipCodec")), None, fileExt=Some("csv.gz"))
  }

  def jobId: String;
  def jobName: String;
  def getReportPath: String;
  def getReportKey: String;

}
