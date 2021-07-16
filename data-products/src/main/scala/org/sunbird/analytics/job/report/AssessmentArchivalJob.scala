package org.sunbird.analytics.job.report

import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.{SparkContext, sql}
import org.apache.spark.sql.cassandra.CassandraSparkSessionFunctions
import org.apache.spark.sql.functions.{col, explode_outer, to_timestamp, weekofyear, year}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig}
import org.sunbird.analytics.exhaust.collection.UDFUtils

import java.util.concurrent.atomic.AtomicInteger

object AssessmentArchivalJob extends optional.Application with IJob with BaseReportsJob {
  val cassandraUrl = "org.apache.spark.sql.cassandra"
  private val assessmentAggDBSettings: Map[String, String] = Map("table" -> "assessment_aggregator", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster")
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
      val res = CommonUtil.time(archiveData(spark, fetchData, jobConfig))
      val total_archived_files = res._2.length
      if (truncateData) deleteRecords(spark, assessmentAggDBSettings.getOrElse("keyspace", "sunbird_courses"), assessmentAggDBSettings.getOrElse("table", "assessment_aggregator")) else JobLogger.log(s"Skipping the ${assessmentAggDBSettings.getOrElse("table", "assessment_aggregator")} truncate process", None, INFO)
      JobLogger.end(s"$jobName completed execution", "SUCCESS", Option(Map("timeTaken" -> res._1, "total_archived_files" -> total_archived_files)))
    } finally {
      frameworkContext.closeContext()
      spark.close()
    }


  }

  def init()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): Unit = {
    spark.setCassandraConf("LMSCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.courses.cluster.host")))
  }

  // $COVERAGE-ON$
  def archiveData(sparkSession: SparkSession, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame, jobConfig: JobConfig): Array[Map[String, Any]] = {
    val assessmentData: DataFrame = getAssessmentData(sparkSession, fetchData)
    print("assessmentData" + assessmentData.show(false))
     print("assessmentData.printSchema()" + assessmentData.printSchema())
    val updatedData = assessmentData.withColumn("updated_on", to_timestamp(col("updated_on")))
      .withColumn("year", year(col("updated_on")))
      .withColumn("week_of_year", weekofyear(col("updated_on")))
      .withColumn("question", UDFUtils.parseResult(col("question")))
//      .withColumn("questiondata",explode_outer(col("question")))
//      .withColumn("questionresponse", UDFUtils.toJSON(col("questiondata.resvalues")))
//      .withColumn("questionoption", UDFUtils.toJSON(col("questiondata.params")))
      //.withColumn("question", UDFUtils.toJSON(col("question")))
    //    assessmentData.coalesce(1)
    //      .write
    //      .partitionBy(partitionCols:_*)
    //      .mode("overwrite")
    //      .format("com.databricks.spark.csv")
    //      .option("header", "true")
    //      .save(AppConf.getConfig("save_path"))
    print("updatedData" + updatedData.show(false))
    val archivedBatchList = updatedData.groupBy(partitionCols.head, partitionCols.tail: _*).count().collect()
    val archivedBatchCount = new AtomicInteger(archivedBatchList.length)
    JobLogger.log(s"Total Batches to Archive By Year & Week $archivedBatchCount", None, INFO)
    val batchesToArchive: Array[BatchPartition] = archivedBatchList.map(f =>
      BatchPartition(f.get(0).asInstanceOf[String], f.get(1).asInstanceOf[Int], f.get(2).asInstanceOf[Int]))
    for (batch <- batchesToArchive) yield {
      val filteredDF = updatedData
        .filter(col("batch_id") === batch.batch_id && col("year") === batch.year && col("week_of_year") === batch.week_of_year)
      upload(filteredDF.drop("year", "week_of_year"), batch, jobConfig)
      val metrics = Map("batch_id" -> batch.batch_id, "year" -> batch.year, "week_of_year" -> batch.week_of_year, "pending_batches" -> archivedBatchCount.getAndDecrement(), "total_records" -> filteredDF.count())
      JobLogger.log(s"Data is archived and Remaining batches to archive is  ", Some(metrics), INFO)
      assessmentData.unpersist()
      metrics
    }
  }

  def getAssessmentData(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame): DataFrame = {
    fetchData(spark, assessmentAggDBSettings, cassandraUrl, new StructType())
  }

  def deleteRecords(sparkSession: SparkSession, keyspace: String, table: String): Unit = {
    //sparkSession.sql(s"TRUNCATE TABLE $keyspace.$table")
    JobLogger.log(s"The Job Cleared The Table Data SuccessFully, Please Execute The Compaction", None, INFO)
  }

  //  def syncToCloud(archivedData: DataFrame, batch: BatchPartition, conf: JobConfig): CompletableFuture[Map[String, Any]] = {
  //    CompletableFuture.supplyAsync(new Supplier[Map[String, Any]]() {
  //      override def get(): Map[String, Any] = {
  //        val res = CommonUtil.time(upload(archivedData, s"${batch.batch_id}-${batch.year}-${batch.week_of_year}", conf))
  //        Map("batch_id" -> batch.batch_id, "year" -> batch.year, "week_of_year" -> batch.week_of_year, "time_taken" -> res._1, "total_records" -> archivedData.count())
  //      }
  //    })
  //  }

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
