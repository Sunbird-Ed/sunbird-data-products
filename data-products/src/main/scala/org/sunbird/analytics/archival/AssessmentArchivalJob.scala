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

  private val partitionCols = List("batch_id", "year", "week_of_year")
  private val columnWithOrder = List("course_id", "batch_id", "user_id", "content_id", "attempt_id", "created_on", "grand_total", "last_attempted_on", "total_max_score", "total_score", "updated_on", "question")

  override def getClassName = "org.sunbird.analytics.archival.AssessmentArchivalJob"
  override def jobName = "AssessmentArchivalJob";
  override def jobId: String = "assessment-archival";
  override def getReportPath = "assessment-archival/";
  override def getReportKey = "assessment";

  override def archiveData(requestConfig: Request, requests: Array[ArchivalRequest])(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): List[ArchivalRequest] = {

    val archivalKeyspace = requestConfig.keyspace.getOrElse(AppConf.getConfig("sunbird.courses.keyspace"))
    val batchId: String = requestConfig.batchId.getOrElse(null)
    val collId: String = requestConfig.collectionId.getOrElse(null)
    val date: String  = requestConfig.date.getOrElse(null)

    var data = loadData(Map("table" -> requestConfig.archivalTable, "keyspace" -> archivalKeyspace, "cluster" -> "LMSCluster"), cassandraUrl, new StructType())

    data = if (batchId.nonEmpty && collId.nonEmpty) {
      data.filter(col("batch_id") === batchId && col("course_id") === collId).persist()
    } else if (batchId.nonEmpty) {
      data.filter(col("batch_id") === batchId).persist()
    } else {
      data
    }
    try {
      val dataDF = generatePeriodInData(data)
      val filteredDF = dataFilter(requests, dataDF)
      val archiveBatchList = filteredDF.groupBy(partitionCols.head, partitionCols.tail: _*).count().collect()
      val batchesToArchive: Map[String, Array[BatchPartition]] = archiveBatchList.map(f => BatchPartition(f.get(0).asInstanceOf[String], Period(f.get(1).asInstanceOf[Int], f.get(2).asInstanceOf[Int]))).groupBy(_.batchId)

      archiveBatches(batchesToArchive, filteredDF, requestConfig)
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        List()
    }
  }

  override def archiveBatches(batchesToArchive: Map[String, Array[BatchPartition]], data: DataFrame, requestConfig: Request)(implicit config: JobConfig): List[ArchivalRequest] = {
    batchesToArchive.flatMap(batches => {
      val processingBatch = new AtomicInteger(batches._2.length)
      JobLogger.log(s"Started Processing to archive the data", Some(Map("batch_id" -> batches._1, "total_part_files_to_archive" -> processingBatch)))

      // Loop through the week_num & year batch partition
      batches._2.map((batch: BatchPartition) => {
        val filteredDF = data.filter(col("batch_id") === batch.batchId && col("year") === batch.period.year && col("week_of_year") === batch.period.weekOfYear).select(columnWithOrder.head, columnWithOrder.tail: _*)
        val collectionId = filteredDF.first().getAs[String]("course_id")
        var archivalRequest = getRequest(collectionId, batch.batchId, batch.period.year, batch.period.weekOfYear)

        if (archivalRequest == null) {
          val request_data = JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(requestConfig)) ++ Map[String, Int](
            "week" -> batch.period.weekOfYear,
            "year"-> batch.period.year
          )
          archivalRequest = ArchivalRequest("", batch.batchId, collectionId, Option(getReportKey), jobId, None, None, null, null, None, Option(0), JSONUtils.serialize(request_data), None)
        }

        try {
          val urls = upload(filteredDF, batch) // Upload the archived files into blob store
          archivalRequest.blob_url = Option(urls)
          JobLogger.log(s"Data is archived and Processing the remaining part files ", None, Level.INFO)
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
    data.withColumn("updated_on", to_timestamp(col("updated_on")))
      .withColumn("year", year(col("updated_on")))
      .withColumn("week_of_year", weekofyear(col("updated_on")))
      .withColumn("question", to_json(col("question")))
  }


}
