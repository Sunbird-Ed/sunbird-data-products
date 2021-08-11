package org.sunbird.analytics.job.report

import com.datastax.spark.connector.cql.CassandraConnectorConf
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

import java.util.concurrent.atomic.AtomicInteger

object AssessmentArchivalJob extends optional.Application with IJob with BaseReportsJob {
  val cassandraUrl = "org.apache.spark.sql.cassandra"
  private val assessmentAggDBSettings: Map[String, String] = Map("table" -> AppConf.getConfig("sunbird.courses.assessment.table"), "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster")
  implicit val className: String = "org.sunbird.analytics.job.report.AssessmentArchivalJob"
  private val partitionCols = List("batch_id", "year", "week_of_year")

  case class BatchPartition(batch_id: String, year: Int, week_of_year: Int)

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
    val truncateData: Boolean = modelParams.getOrElse("truncateData", "false").asInstanceOf[Boolean]
    init()
    try {
      val res = CommonUtil.time(archiveData(spark, jobConfig))
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
    val modelParams = jobConfig.modelParams.get
    val deleteArchivedBatch: Boolean = modelParams.getOrElse("deleteArchivedBatch", false).asInstanceOf[Boolean]
    val batches: List[String] = AppConf.getConfig("assessment.batches").split(",").toList.filter(x => x.nonEmpty)

    val assessmentDF: DataFrame = getAssessmentData(sparkSession, batches)
    val assessmentData = assessmentDF.withColumn("updated_on", to_timestamp(col("updated_on")))
      .withColumn("year", year(col("updated_on")))
      .withColumn("week_of_year", weekofyear(col("updated_on")))
      .withColumn("question", to_json(col("question")))

    val archiveBatchList = assessmentData.groupBy(partitionCols.head, partitionCols.tail: _*).count().collect()
    val batchesToArchiveCount = new AtomicInteger(archiveBatchList.length)
    JobLogger.log(s"Total Batches to Archive By Year & Week $batchesToArchiveCount", None, INFO)

    val batchesToArchive: Map[String, Array[BatchPartition]] = archiveBatchList.map(f => BatchPartition(f.get(0).asInstanceOf[String], f.get(1).asInstanceOf[Int], f.get(2).asInstanceOf[Int])).groupBy(_.batch_id)

    batchesToArchive.flatMap(batches => {
      val processingBatch = new AtomicInteger(batches._2.length)
      JobLogger.log(s"Started Processing to archive the data", Some(Map("batch_id" -> batches._1, "total_part_files_to_archive" -> batches._2.length)), INFO)
      val res = for (batch <- batches._2) yield {
        val filteredDF = assessmentData.filter(col("batch_id") === batch.batch_id && col("year") === batch.year && col("week_of_year") === batch.week_of_year)
        upload(filteredDF.drop("year", "week_of_year"), batch, jobConfig)
        val metrics = Map("batch_id" -> batch.batch_id, "year" -> batch.year, "week_of_year" -> batch.week_of_year, "pending_part_files" -> processingBatch.getAndDecrement(), "total_records" -> filteredDF.count())
        JobLogger.log(s"Data is archived and Processing the remaining part files ", Some(metrics), INFO)
        assessmentData.unpersist()
        metrics
      }
      if(deleteArchivedBatch) removeRecords(sparkSession, batches._1) else JobLogger.log(s"Skipping the batch deletions ${batches._1}", None, INFO)
      JobLogger.log(s"The data archival is successful", Some(Map("batch_id" -> batches._1, "pending_batches" -> batchesToArchiveCount.getAndDecrement())), INFO)
      res
    }).toArray
  }

  def removeRecords(sparkSession: SparkSession, batchId: String): Unit = {
    sparkSession.sql(s"DELETE FROM ${AppConf.getConfig("sunbird.courses.keyspace")}.${AppConf.getConfig("sunbird.courses.assessment.table")} WHERE batch_id = $batchId")
    JobLogger.log(s"Deleting the records for the batch $batchId from the DB", None, INFO)
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
    val fileName = s"${batch.batch_id}-${batch.year}-${batch.week_of_year}"
    val storageConfig = getStorageConfig(
      container,
      objectKey,
      jobConfig)
    JobLogger.log(s"Uploading reports to blob storage", None, INFO)
    archivedData.saveToBlobStore(storageConfig, "csv", s"$reportPath$fileName-${System.currentTimeMillis()}", Option(Map("header" -> "true")), None)
  }

}
