package org.sunbird.analytics.job.report

import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.{SomeColumns, toRDDFunctions}
import org.apache.spark.SparkContext
import org.apache.spark.sql.cassandra.CassandraSparkSessionFunctions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig}
import org.joda.time.DateTime
import org.sunbird.analytics.exhaust.util.ExhaustUtil

import java.util.concurrent.atomic.AtomicInteger

object AssessmentArchivalJob extends optional.Application with IJob with BaseReportsJob {
  val cassandraUrl = "org.apache.spark.sql.cassandra"
  private val assessmentAggDBSettings: Map[String, String] = Map("table" -> AppConf.getConfig("sunbird.courses.assessment.table"), "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster")
  implicit val className: String = "org.sunbird.analytics.job.report.AssessmentArchivalJob"
  private val partitionCols = List("batch_id", "year", "week_of_year")
  private val columnWithOrder = List("course_id", "batch_id", "user_id", "content_id", "attempt_id", "created_on", "grand_total", "last_attempted_on", "total_max_score", "total_score", "updated_on", "question")

  case class Period(year: Int,
                    weekOfYear: Int)

  case class BatchPartition(batchId: String, period: Period)


  case class ArchivalMetrics(batchId: Option[String],
                             period: Period,
                             totalArchivedRecords: Option[Long],
                             pendingWeeksOfYears: Option[Long],
                             totalDeletedRecords: Option[Long],
                             totalDistinctBatches: Long
                            )


  // $COVERAGE-OFF$ Disabling scoverage for main and execute method
  override def main(config: String)(implicit sc: Option[SparkContext], fc: Option[FrameworkContext]): Unit = {
    implicit val className: String = "org.sunbird.analytics.job.report.AssessmentArchivalJob"
    val jobName = "AssessmentArchivalJob"
    JobLogger.init(jobName)
    JobLogger.start(s"$jobName started executing", Option(Map("config" -> config, "model" -> jobName)))
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](config)
    implicit val spark: SparkSession = openSparkSession(jobConfig)
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()
    val modelParams = jobConfig.modelParams.get
    val deleteArchivedBatch: Boolean = modelParams.getOrElse("deleteArchivedBatch", false).asInstanceOf[Boolean]
    init()
    try {

      /**
       * Below date and batchId configs are optional. By default,
       * The Job will remove the records for the last week.
       */
      val date = modelParams.getOrElse("date", null).asInstanceOf[String]
      val batchIds = modelParams.getOrElse("batchIds", null).asInstanceOf[List[String]]
      val res = if (deleteArchivedBatch) CommonUtil.time(removeRecords(date, Some(batchIds))) else CommonUtil.time(archiveData(Some(batchIds)))
      val total_archived_files = res._2.length
      JobLogger.end(s"$jobName completed execution", "SUCCESS", Option(Map("timeTaken" -> res._1, "archived_details" -> res._2, "total_archived_files" -> total_archived_files)))
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        JobLogger.end(s"$jobName completed execution with the error ${ex.getMessage}", "FAILED", None)
    } finally {
      frameworkContext.closeContext()
      spark.close()
    }
  }

  def init()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): Unit = {
    spark.setCassandraConf("LMSCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.courses.cluster.host")))
  }

  // $COVERAGE-ON$
  def archiveData(batchIds: Option[List[String]])(implicit spark: SparkSession, config: JobConfig): Array[ArchivalMetrics] = {
    // Get the assessment Data
    val assessmentDF: DataFrame = getAssessmentData(spark, batchIds.getOrElse(List()))

    //Get the Week Num & Year Value for Based on the updated_on value column
    val assessmentData = assessmentDF.withColumn("updated_on", to_timestamp(col("updated_on")))
      .withColumn("year", year(col("updated_on")))
      .withColumn("week_of_year", weekofyear(col("updated_on")))
      .withColumn("question", to_json(col("question")))

    val archiveBatchList = assessmentData.groupBy(partitionCols.head, partitionCols.tail: _*).count().collect()
    val totalBatchesToArchive = new AtomicInteger(archiveBatchList.length)
    JobLogger.log(s"Total Batches to Archive By Year & Week $totalBatchesToArchive", None, INFO)

    // Loop through the batches to archive list
    val batchesToArchive: Map[String, Array[BatchPartition]] = archiveBatchList.map(f => BatchPartition(f.get(0).asInstanceOf[String], Period(f.get(1).asInstanceOf[Int], f.get(2).asInstanceOf[Int]))).groupBy(_.batchId)
    val archivalStatus: Array[ArchivalMetrics] = batchesToArchive.flatMap(batches => {
      val processingBatch = new AtomicInteger(batches._2.length)
      JobLogger.log(s"Started Processing to archive the data", Some(Map("batch_id" -> batches._1, "total_part_files_to_archive" -> batches._2.length)), INFO)
      // Loop through the week_num & year batch partition
      val res = for (batch <- batches._2.asInstanceOf[Array[BatchPartition]]) yield {
        val filteredDF = assessmentData.filter(col("batch_id") === batch.batchId && col("year") === batch.period.year && col("week_of_year") === batch.period.weekOfYear).select(columnWithOrder.head, columnWithOrder.tail: _*)
        upload(filteredDF, batch, config)

        val metrics = ArchivalMetrics(batchId = Some(batch.batchId), Period(year = batch.period.year, weekOfYear = batch.period.weekOfYear),
          pendingWeeksOfYears = Some(processingBatch.getAndDecrement()), totalArchivedRecords = Some(filteredDF.count()), totalDeletedRecords = None, totalDistinctBatches = filteredDF.select("batch_id").distinct().count())

        JobLogger.log(s"Data is archived and Processing the remaining part files ", Some(metrics), INFO)
        metrics
      }
      JobLogger.log(s"${batches._1} is successfully archived", Some(Map("batch_id" -> batches._1, "pending_batches" -> totalBatchesToArchive.getAndDecrement())), INFO)
      res
    }).toArray
    assessmentData.unpersist()
    archivalStatus
  }

  // Delete the records for the archived batch data.
  // Date - YYYY-MM-DD Format
  // Batch IDs are optional
  def removeRecords(date: String, batchIds: Option[List[String]])(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): Array[ArchivalMetrics] = {
    val period: Period = getWeekAndYearVal(date) // Date is optional, By default it will provide the previous week num of current year
    val res = if (batchIds.nonEmpty) {
      for (batchId <- batchIds.getOrElse(List())) yield {
        remove(period, Some(batchId))
      }
    } else {
      List(remove(period, None))
    }
    res.toArray
  }

  def remove(period: Period, batchId: Option[String])(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): ArchivalMetrics = {
    val archivedDataDF = fetchArchivedBatches(period, batchId)
    val archivedData = archivedDataDF.select("course_id", "batch_id", "user_id", "content_id", "attempt_id")
    val totalArchivedRecords: Long = archivedData.count
    val totalDistinctBatches: Long = archivedData.select("batch_id").distinct().count()
    JobLogger.log(s"Deleting $totalArchivedRecords records for the year ${period.year} and week of year ${period.weekOfYear} from the DB ", None, INFO)
    archivedData.rdd.deleteFromCassandra(AppConf.getConfig("sunbird.courses.keyspace"), AppConf.getConfig("sunbird.courses.assessment.table"), keyColumns = SomeColumns("course_id", "batch_id", "user_id", "content_id", "attempt_id"))
    ArchivalMetrics(batchId = batchId, Period(year = period.year, weekOfYear = period.weekOfYear),
      pendingWeeksOfYears = None, totalArchivedRecords = Some(totalArchivedRecords), totalDeletedRecords = Some(totalArchivedRecords), totalDistinctBatches = totalDistinctBatches)
  }

  def fetchArchivedBatches(period: Period, batchId: Option[String])(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {
    val azureFetcherConfig = config.modelParams.get("archivalFetcherConfig").asInstanceOf[Map[String, AnyRef]]
    val store = azureFetcherConfig("store").asInstanceOf[String]
    val format: String = azureFetcherConfig.getOrElse("blobExt", "csv.gz").asInstanceOf[String]
    val filePath = azureFetcherConfig.getOrElse("reportPath", "archived-data/").asInstanceOf[String]
    val container = azureFetcherConfig.getOrElse("container", "reports").asInstanceOf[String]
    val blobFields = Map("year" -> period.year.toString, "weekNum" -> period.weekOfYear.toString, "batchId" -> batchId.orNull)
    JobLogger.log(s"Fetching a archived records", Some(blobFields), INFO)
    ExhaustUtil.getArchivedData(store, filePath, container, blobFields, Some(format))
  }

  def getAssessmentData(spark: SparkSession, batchIds: List[String]): DataFrame = {
    import spark.implicits._
    val assessmentDF = fetchData(spark, assessmentAggDBSettings, cassandraUrl, new StructType())
    if (batchIds.nonEmpty) {
      if (batchIds.size > 1) {
        val batchListDF = batchIds.toDF("batch_id")
        assessmentDF.join(batchListDF, Seq("batch_id"), "inner").persist()
      }
      else {
        assessmentDF.filter(col("batch_id") === batchIds).persist()
      }

    } else {
      assessmentDF
    }
  }

  def upload(archivedData: DataFrame,
             batch: BatchPartition,
             jobConfig: JobConfig): List[String] = {
    val modelParams = jobConfig.modelParams.get
    val reportPath: String = modelParams.getOrElse("reportPath", "archived-data/").asInstanceOf[String]
    val container = AppConf.getConfig("cloud.container.reports")
    val objectKey = AppConf.getConfig("course.metrics.cloud.objectKey")
    val fileName = s"${batch.batchId}/${batch.period.year}-${batch.period.weekOfYear}"
    val storageConfig = getStorageConfig(
      container,
      objectKey,
      jobConfig)
    JobLogger.log(s"Uploading reports to blob storage", None, INFO)
    archivedData.saveToBlobStore(storageConfig = storageConfig, format = "csv", reportId = s"$reportPath$fileName-${System.currentTimeMillis()}", options = Option(Map("header" -> "true", "codec" -> "org.apache.hadoop.io.compress.GzipCodec")), partitioningColumns = None, fileExt = Some("csv.gz"))
  }
  // Date - YYYY-MM-DD Format
  def getWeekAndYearVal(date: String): Period = {
    if (null != date && date.nonEmpty) {
      val dt = new DateTime(date)
      Period(year = dt.getYear, weekOfYear = dt.getWeekOfWeekyear)
    } else {
      val today = new DateTime()
      val lastWeek = today.minusWeeks(1) // Get always for the previous week of the current
      Period(year = lastWeek.getYear, weekOfYear = lastWeek.getWeekOfWeekyear)
    }
  }

}
