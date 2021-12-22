package org.sunbird.analytics.archival

import com.datastax.spark.connector.{SomeColumns, toRDDFunctions}
import org.apache.spark.sql.functions.{col, concat, lit, to_json, to_timestamp, weekofyear, year}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig, Level}
import org.sunbird.analytics.archival.util.ArchivalRequest

import java.util.concurrent.atomic.AtomicInteger
import org.apache.spark.sql.functions._
import org.sunbird.analytics.exhaust.util.ExhaustUtil
import org.sunbird.analytics.util.Constants

import scala.collection.immutable.List

case class CollectionDetails(result: Result)
case class Result(content: List[CollectionInfo])
case class CollectionInfo(identifier: String)

object AssessmentArchivalJob extends optional.Application with BaseArchivalJob {

  case class Period(year: Int, weekOfYear: Int)
  case class BatchPartition(collectionId: String, batchId: String, period: Period)
  case class ArchivalMetrics(batch: BatchPartition,
                             totalArchivedRecords: Option[Long],
                             pendingWeeksOfYears: Option[Long],
                             totalDeletedRecords: Option[Long]
                            )

  private val partitionCols = List("course_id", "batch_id", "year", "week_of_year")
  private val columnWithOrder = List("course_id", "batch_id", "user_id", "content_id", "attempt_id", "created_on", "grand_total", "last_attempted_on", "total_max_score", "total_score", "updated_on", "question")

  override def getClassName = "org.sunbird.analytics.archival.AssessmentArchivalJob"
  override def jobName = "AssessmentArchivalJob"
  override def jobId: String = "assessment-archival"
  override def getReportPath = "assessment-archival/"
  override def getReportKey = "assessment"
  override def dateColumn = "updated_on"

  override def archivalFormat(batch: Map[String,AnyRef]): String = {
    val formatDetails = JSONUtils.deserialize[BatchPartition](JSONUtils.serialize(batch))
    s"${formatDetails.batchId}_${formatDetails.collectionId}/${formatDetails.period.year}-${formatDetails.period.weekOfYear}"
  }

  override def dataFilter(requests: Array[ArchivalRequest], dataDF: DataFrame): DataFrame = {
    var filteredDF = dataDF
    for (request <- requests) {
      if (request.archival_status.equals("SUCCESS")) {
        val request_data = JSONUtils.deserialize[Map[String, AnyRef]](request.request_data)
        filteredDF = dataDF.filter(
          col("batch_id").equalTo(request.batch_id) &&
          concat(col("year"), lit("-"), col("week_of_year")) =!= lit(request_data("year") + "-" + request_data("week"))
        )
      }
    }
    filteredDF
  }

  override def archiveData(requestConfig: Request, requests: Array[ArchivalRequest])(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): List[ArchivalRequest] = {
    try {
      val archivalKeyspace = requestConfig.keyspace.getOrElse(AppConf.getConfig("sunbird.courses.keyspace"))
      val date: String  = requestConfig.date.getOrElse(null)

      var data = loadData(Map("table" -> requestConfig.archivalTable, "keyspace" -> archivalKeyspace, "cluster" -> "LMSCluster"), cassandraUrl, new StructType())
      data = validateBatch(data, requestConfig.batchId, requestConfig.collectionId, requestConfig.batchFilters, requestConfig.query)

      val dataDF = generatePeriodInData(data)
      val filteredDF = dataFilter(requests, dataDF)

      val archiveBatchList = filteredDF.groupBy(partitionCols.head, partitionCols.tail: _*).count().collect()
      val batchesToArchive: Map[String, Array[BatchPartition]] = archiveBatchList.map(f => BatchPartition(f.get(0).asInstanceOf[String], f.get(1).asInstanceOf[String], Period(f.get(2).asInstanceOf[Int], f.get(3).asInstanceOf[Int]))).groupBy(_.batchId)

      archiveBatches(batchesToArchive, filteredDF, requestConfig)
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        JobLogger.log("archiveData: Exception with error message = " + ex.getMessage, None, Level.ERROR)
        List()
    }
  }

  def validateBatch(data: DataFrame,batchid: Option[String], collectionid: Option[String], batchFilters: Option[List[String]], searchFilter: Option[Map[String, AnyRef]])(implicit spark: SparkSession, fc: FrameworkContext): DataFrame ={
    implicit val sqlContext = new SQLContext(spark.sparkContext)
    import sqlContext.implicits._

    val filteredDF = if(batchid.isDefined && collectionid.isDefined) {
      data.filter(col("batch_id") === batchid.get && col("course_id") === collectionid.get).persist()
    } else if (batchFilters.isDefined) {
      val batch = batchFilters.get.toDF()
      data.join(batch, Seq("batch_id"), "inner")
    } else {
      JobLogger.log("Neither batchId nor batchFilters present", None, Level.INFO)
      data
    }
    if (searchFilter.isDefined) {
      val res = searchContent(searchFilter.get)
      filteredDF.join(res, col("course_id") === col("identifier"), "inner")
    } else filteredDF

  }

  def searchContent(searchFilter: Map[String, AnyRef])(implicit spark: SparkSession, fc: FrameworkContext): DataFrame = {
    // TODO: Handle limit and do a recursive search call
    val apiURL = Constants.COMPOSITE_SEARCH_URL
    val request = JSONUtils.serialize(searchFilter)
    val response = RestUtil.post[CollectionDetails](apiURL, request).result.content
    spark.createDataFrame(response).select("identifier")
  }

  def archiveBatches(batchesToArchive: Map[String, Array[BatchPartition]], data: DataFrame, requestConfig: Request)(implicit config: JobConfig): List[ArchivalRequest] = {
    batchesToArchive.flatMap(batches => {
      val processingBatch = new AtomicInteger(batches._2.length)
      JobLogger.log(s"Started Processing to archive the data", Some(Map("batch_id" -> batches._1, "total_part_files_to_archive" -> processingBatch)))

      // Loop through the week_num & year batch partition
      batches._2.map((batch: BatchPartition) => {
        val filteredDF = data.filter(
          col("course_id") === batch.collectionId &&
          col("batch_id") === batch.batchId &&
          col("year") === batch.period.year &&
          col("week_of_year") === batch.period.weekOfYear
        ).withColumn("last_attempted_on", tsToLongUdf(col("last_attempted_on")))
         .withColumn("updated_on", tsToLongUdf(col("updated_on")))
         .select(columnWithOrder.head, columnWithOrder.tail: _*)
        var archivalRequest:ArchivalRequest = getRequest(batch.collectionId, batch.batchId, List(batch.period.year, batch.period.weekOfYear))

        if (archivalRequest == null) {
          val request_data = JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(requestConfig)) ++ Map[String, Int](
            "week" -> batch.period.weekOfYear,
            "year"-> batch.period.year
          )
          archivalRequest = ArchivalRequest("", batch.batchId, batch.collectionId, Option(getReportKey), jobId, None, None, null, null, None, Option(0), JSONUtils.serialize(request_data), None)
        }

        try {
          val urls = upload(filteredDF, JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(batch))) // Upload the archived files into blob store
          archivalRequest.blob_url = Option(urls)
          val metrics = ArchivalMetrics(batch, pendingWeeksOfYears = Some(processingBatch.getAndDecrement()), totalArchivedRecords = Some(filteredDF.count()), totalDeletedRecords = None)
          JobLogger.log(s"Data is archived and Processing the remaining part files ", Some(metrics), Level.INFO)
          markArchivalRequestAsSuccess(archivalRequest, requestConfig)
        } catch {
          case ex: Exception => {
            JobLogger.log("archiveBatch: Exception with error message = " + ex.getLocalizedMessage, Some(batch), Level.ERROR)
            markArchivalRequestAsFailed(archivalRequest, ex.getLocalizedMessage)
          }
        }
      })
    }).toList
  }

  def loadArchivedData(batch: BatchPartition)(implicit spark: SparkSession, fc: FrameworkContext, jobConfig: JobConfig): DataFrame = {
    val azureFetcherConfig = jobConfig.modelParams.get("assessmentFetcherConfig").asInstanceOf[Map[String, AnyRef]]

    val store = azureFetcherConfig("store").asInstanceOf[String]
    val format:String = azureFetcherConfig.getOrElse("format", "csv").asInstanceOf[String]
    val filePath = azureFetcherConfig.getOrElse("filePath", "archival-data/").asInstanceOf[String]
    val container = azureFetcherConfig.getOrElse("container", "reports").asInstanceOf[String]

    ExhaustUtil.getArchivedData(store, filePath, container, Map("batchId" -> batch.batchId, "collectionId"-> batch.collectionId, "year" -> batch.period.year, "weekNum" -> batch.period.weekOfYear), Option(format))
  }

  override def deleteArchivedData(requestConfig: Request, requests: Array[ArchivalRequest])(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): List[ArchivalRequest] = {
    requests.filter(r => r.archival_status.equals("SUCCESS")).map((request: ArchivalRequest) => {
      deleteBatch(requestConfig, request)
    }).toList
  }

  def deleteBatch(requestConfig: Request, request: ArchivalRequest)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): ArchivalRequest = {
    try {
      val request_data = JSONUtils.deserialize[Map[String, AnyRef]](request.request_data)
      val batchPartition = BatchPartition(request.collection_id, request.batch_id, Period(request_data("year").asInstanceOf[Int], request_data("week").asInstanceOf[Int]))
      val archivedData = loadArchivedData(batchPartition).select("course_id", "batch_id", "user_id", "content_id", "attempt_id")

      val totalArchivedRecords: Long = archivedData.count
      JobLogger.log(s"Deleting $totalArchivedRecords archived records only, for the year ${batchPartition.period.year} and week of year ${batchPartition.period.weekOfYear} from the DB ", None, Level.INFO)

      archivedData.rdd.deleteFromCassandra(AppConf.getConfig("sunbird.courses.keyspace"), AppConf.getConfig("sunbird.courses.assessment.table"), keyColumns = SomeColumns("course_id", "batch_id", "user_id", "content_id", "attempt_id"))
      val metrics = ArchivalMetrics(batchPartition, pendingWeeksOfYears = None, totalArchivedRecords = Some(totalArchivedRecords), totalDeletedRecords = Some(totalArchivedRecords))

      JobLogger.log(s"Data is archived and Processing the remaining part files ", Some(metrics), Level.INFO)
      markDeletionRequestAsSuccess(request, requestConfig)
    } catch {
      case ex: Exception => {
        JobLogger.log("deleteBatch: Exception with error message = " + ex.getLocalizedMessage, Some(request), Level.ERROR)
        markDeletionRequestAsFailed(request, ex.getLocalizedMessage)
      }
    }
  }

  def generatePeriodInData(data: DataFrame): DataFrame = {
    data.withColumn("updated_on", to_timestamp(col(dateColumn)))
      .withColumn("year", year(col("updated_on")))
      .withColumn("week_of_year", weekofyear(col("updated_on")))
      .withColumn("question", to_json(col("question")))
  }
}
