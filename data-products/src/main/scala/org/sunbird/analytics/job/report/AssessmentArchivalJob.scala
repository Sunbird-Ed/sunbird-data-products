package org.sunbird.analytics.job.report

import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.toRDDFunctions
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
import org.joda.time.{DateTime, LocalDate}
import org.sunbird.analytics.exhaust.util.ExhaustUtil

import java.util.concurrent.atomic.AtomicInteger

object AssessmentArchivalJob extends optional.Application with IJob with BaseReportsJob {
  val cassandraUrl = "org.apache.spark.sql.cassandra"
  private val assessmentAggDBSettings: Map[String, String] = Map("table" -> AppConf.getConfig("sunbird.courses.assessment.table"), "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster")
  implicit val className: String = "org.sunbird.analytics.job.report.AssessmentArchivalJob"
  private val partitionCols = List("batch_id", "year", "week_of_year")
  private val columnWithOrder = List("course_id", "batch_id", "user_id", "content_id", "attempt_id", "created_on", "grand_total", "last_attempted_on", "total_max_score", "total_score", "updated_on", "question")

  case class BatchPartition(batch_id: String, year: Int, week_of_year: Int)

  case class Period(year: Int, week_of_year: Int)

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

      // Below config is optional, If the user wants to removed the archived data for a specific date or Batch Id
      // NOTE - By default the records will be removed only for the previous week num of archived data only.
      val archivedDate = modelParams.getOrElse("archivedDate", null).asInstanceOf[String]
      val archivedBatch = modelParams.getOrElse("archivedBatch", null).asInstanceOf[String]

      val res = if (deleteArchivedBatch) CommonUtil.time(removeRecords(archivedDate, Some(archivedBatch))) else CommonUtil.time(archiveData(spark, jobConfig))
      val total_archived_files = res._2.length
      JobLogger.end(s"$jobName completed execution", "SUCCESS", Option(Map("timeTaken" -> res._1, "total_archived_files" -> total_archived_files)))
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        JobLogger.end(s"$jobName completed execution with the error ${ex.getMessage}", "FAILED", None)
      }
    } finally {
      frameworkContext.closeContext()
      spark.close()
    }
  }

  def init()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): Unit = {
    spark.setCassandraConf("LMSCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.courses.cluster.host")))
  }

  // $COVERAGE-ON$
  def archiveData(sparkSession: SparkSession, jobConfig: JobConfig): Array[Map[String, Any]] = {
    // Read the batches information to archive
    val batches: List[String] = AppConf.getConfig("assessment.batches").split(",").toList.filter(x => x.nonEmpty)

    // Get the assessment Data
    val assessmentDF: DataFrame = getAssessmentData(sparkSession, batches)

    //Get the Week Num & Year Value for Based on the updated_on value column
    val assessmentData = assessmentDF.withColumn("updated_on", to_timestamp(col("updated_on")))
      .withColumn("year", year(col("updated_on")))
      .withColumn("week_of_year", weekofyear(col("updated_on")))
      .withColumn("question", to_json(col("question")))

    val archiveBatchList = assessmentData.groupBy(partitionCols.head, partitionCols.tail: _*).count().collect()
    val batchesToArchiveCount = new AtomicInteger(archiveBatchList.length)
    JobLogger.log(s"Total Batches to Archive By Year & Week $batchesToArchiveCount", None, INFO)

    // Loop through the batches to archive list
    val batchesToArchive: Map[String, Array[BatchPartition]] = archiveBatchList.map(f => BatchPartition(f.get(0).asInstanceOf[String], f.get(1).asInstanceOf[Int], f.get(2).asInstanceOf[Int])).groupBy(_.batch_id)
    batchesToArchive.flatMap(batches => {
      val processingBatch = new AtomicInteger(batches._2.length)
      JobLogger.log(s"Started Processing to archive the data", Some(Map("batch_id" -> batches._1, "total_part_files_to_archive" -> batches._2.length)), INFO)
      // Loop through the week_num & year batch partition
      val res = for (batch <- batches._2) yield {
        val filteredDF = assessmentData.filter(col("batch_id") === batch.batch_id && col("year") === batch.year && col("week_of_year") === batch.week_of_year).select(columnWithOrder.head, columnWithOrder.tail: _*)
        upload(filteredDF, batch, jobConfig)
        val metrics = Map("batch_id" -> batch.batch_id, "year" -> batch.year, "week_of_year" -> batch.week_of_year, "pending_part_files" -> processingBatch.getAndDecrement(), "total_records" -> filteredDF.count())
        JobLogger.log(s"Data is archived and Processing the remaining part files ", Some(metrics), INFO)
        assessmentData.unpersist()
        metrics
      }
      JobLogger.log(s"${batches._1} is successfully archived", Some(Map("batch_id" -> batches._1, "pending_batches" -> batchesToArchiveCount.getAndDecrement())), INFO)
      res
    }).toArray
  }

  // Delete the records for the archived batch data.
  // Date - YYYY-MM-DD Format
  def removeRecords(date: String, batchId: Option[String])(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): Array[Map[String, Any]] = {
    val period: Period = getWeekAndYearVal(date)
    val archivedDataDF = fetchArchivedBatches(period, batchId)
    val archivedDataRDD = archivedDataDF.select("course_id", "batch_id", "user_id", "content_id", "attempt_id").rdd
    val totalArchivedRecords = archivedDataRDD.count
    archivedDataRDD.deleteFromCassandra(AppConf.getConfig("sunbird.courses.keyspace"), AppConf.getConfig("sunbird.courses.assessment.table"))
    JobLogger.log(s"Deleted $totalArchivedRecords records for the batch from the DB", None, INFO)
    Array(Map("deleted_records" -> totalArchivedRecords))
  }

  // Date - YYYY-MM-DD Format
  def getWeekAndYearVal(date: String): Period = {
    if (null != date && date.nonEmpty) {
      val dt = new DateTime(date)
      Period(year = dt.getYear, week_of_year = dt.getWeekOfWeekyear)
    } else {
      val today = new DateTime()
      val lastWeek = today.minusWeeks(1) // Get always for the previous week of the current
      Period(year = lastWeek.getYear, week_of_year = lastWeek.getWeekOfWeekyear)
    }
  }

  def fetchArchivedBatches(period: Period, batchId: Option[String])(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {
    val azureFetcherConfig = config.modelParams.get("archivalFetcherConfig").asInstanceOf[Map[String, AnyRef]]
    val store = azureFetcherConfig("store").asInstanceOf[String]
    val format: String = azureFetcherConfig.getOrElse("format", "csv.gz").asInstanceOf[String]
    val filePath = azureFetcherConfig.getOrElse("filePath", "archival-data/").asInstanceOf[String]
    val container = azureFetcherConfig.getOrElse("container", "reports").asInstanceOf[String]
    val blobFields = Map("year" -> period.year.toString, "weekNum" -> period.week_of_year.toString, "batchId" -> batchId.orNull)
    JobLogger.log(s"Fetching a archived records", Some(blobFields), INFO)
    ExhaustUtil.getArchivedData(store, filePath, container, blobFields, Some(format))
  }

  def getAssessmentData(spark: SparkSession, batchIds: List[String]): DataFrame = {
    import spark.implicits._
    val assessmentDF = fetchData(spark, assessmentAggDBSettings, cassandraUrl, new StructType())
    if (batchIds.nonEmpty) {
      val batchListDF = batchIds.asInstanceOf[List[String]].toDF("batch_id")
      assessmentDF.join(batchListDF, Seq("batch_id"), "inner").persist()
    } else {
      assessmentDF
    }
  }

  def upload(archivedData: DataFrame,
             batch: BatchPartition,
             jobConfig: JobConfig): List[String] = {
    val modelParams = jobConfig.modelParams.get
    val reportPath: String = modelParams.getOrElse("reportPath", "archival-data/").asInstanceOf[String]
    val container = AppConf.getConfig("cloud.container.reports")
    val objectKey = AppConf.getConfig("course.metrics.cloud.objectKey")
    val fileName = s"${batch.batch_id}/${batch.year}-${batch.week_of_year}"
    val storageConfig = getStorageConfig(
      container,
      objectKey,
      jobConfig)
    JobLogger.log(s"Uploading reports to blob storage", None, INFO)
    archivedData.saveToBlobStore(storageConfig = storageConfig, format = "csv", reportId = s"$reportPath$fileName-${System.currentTimeMillis()}", options = Option(Map("header" -> "true", "codec" -> "org.apache.hadoop.io.compress.GzipCodec")), partitioningColumns = None, fileExt = Some("csv.gz"))
  }

}
