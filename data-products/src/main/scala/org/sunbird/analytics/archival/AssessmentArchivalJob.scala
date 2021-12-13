package org.sunbird.analytics.archival

import org.apache.spark.sql.functions.{col, concat, lit, to_json, to_timestamp, weekofyear, year}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig, Level}
import org.sunbird.analytics.archival.util.ArchivalRequest

import java.util.concurrent.atomic.AtomicInteger

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
    s"${formatDetails.batchId}/${formatDetails.period.year}-${formatDetails.period.weekOfYear}"
  }

  override def dataFilter(requests: Array[ArchivalRequest], dataDF: DataFrame): DataFrame = {
    var filteredDF = dataDF
    for (request <- requests) {
      if (request.archival_status.equals("SUCCESS")) {
        val request_data = JSONUtils.deserialize[Map[String, AnyRef]](request.request_data)
        filteredDF = dataDF.filter(
          col("batch_id").equalTo(request.batch_id) &&
            concat(col("year"), lit("-"), col("week_of_year")) =!= lit(request_data.get("year").get + "-" + request_data.get("week").get)
        )
      }
    }
    filteredDF
  }

  override def archiveData(requestConfig: Request, requests: Array[ArchivalRequest])(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): List[ArchivalRequest] = {

    val archivalKeyspace = requestConfig.keyspace.getOrElse(AppConf.getConfig("sunbird.courses.keyspace"))
    val batchId: String = requestConfig.batchId.getOrElse("")
    val collectionId: String = requestConfig.collectionId.getOrElse("")
    val date: String  = requestConfig.date.getOrElse(null)

    var data = loadData(Map("table" -> requestConfig.archivalTable, "keyspace" -> archivalKeyspace, "cluster" -> "LMSCluster"), cassandraUrl, new StructType())

    if(collectionId.nonEmpty) {
      data = data.filter(col("course_id") === collectionId)
    }

    if (batchId.nonEmpty) {
      data = data.filter(col("batch_id") === batchId).persist()
    }

    try {
      val dataDF = generatePeriodInData(data)
      val filteredDF = dataFilter(requests, dataDF)
      val archiveBatchList = filteredDF.groupBy(partitionCols.head, partitionCols.tail: _*).count().collect()
      val batchesToArchive: Map[String, Array[BatchPartition]] = archiveBatchList.map(f => BatchPartition(f.get(0).asInstanceOf[String], f.get(1).asInstanceOf[String], Period(f.get(2).asInstanceOf[Int], f.get(3).asInstanceOf[Int]))).groupBy(_.batchId)

      archiveBatches(batchesToArchive, filteredDF, requestConfig)
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        List()
    }
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
        ).select(columnWithOrder.head, columnWithOrder.tail: _*)
        val collectionId = filteredDF.first().getAs[String]("course_id")
        var archivalRequest:ArchivalRequest = getRequest(collectionId, batch.batchId, List(batch.period.year, batch.period.weekOfYear))

        if (archivalRequest == null) {
          val request_data = JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(requestConfig)) ++ Map[String, Int](
            "week" -> batch.period.weekOfYear,
            "year"-> batch.period.year
          )
          archivalRequest = ArchivalRequest("", batch.batchId, collectionId, Option(getReportKey), jobId, None, None, null, null, None, Option(0), JSONUtils.serialize(request_data), None)
        }

        try {
          val urls = upload(filteredDF, Map("batchId" -> batch.batchId, "period"-> Map("year" -> batch.period.year, "weekOfYear" -> batch.period.weekOfYear))) // Upload the archived files into blob store
          archivalRequest.blob_url = Option(urls)
          val metrics = ArchivalMetrics(batch, pendingWeeksOfYears = Some(processingBatch.getAndDecrement()), totalArchivedRecords = Some(filteredDF.count()), totalDeletedRecords = None)
          JobLogger.log(s"Data is archived and Processing the remaining part files ", Some(metrics), Level.INFO)
          markRequestAsSuccess(archivalRequest, requestConfig)
        } catch {
          case ex: Exception => {
            markArchivalRequestAsFailed(archivalRequest, ex.getLocalizedMessage)
          }
        }
      })
    }).toList
  }

  def deleteArchivedData(archivalRequest: Request): List[ArchivalRequest] = {
    // TODO: Deletion feature
    List()
  }

  def generatePeriodInData(data: DataFrame): DataFrame = {
    data.withColumn("updated_on", to_timestamp(col(dateColumn)))
      .withColumn("year", year(col("updated_on")))
      .withColumn("week_of_year", weekofyear(col("updated_on")))
      .withColumn("question", to_json(col("question")))
  }
}
