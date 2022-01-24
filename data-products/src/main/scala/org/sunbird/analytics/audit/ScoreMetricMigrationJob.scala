package org.sunbird.analytics.audit

import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.sql.cassandra.CassandraSparkSessionFunctions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.{SparkContext, sql}
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig}
import org.sunbird.analytics.job.report.BaseReportsJob

import java.util.Date

object ScoreMetricMigrationJob extends optional.Application with IJob with BaseReportsJob {
  val userActivityAggDBSettings = Map("table" -> "user_activity_agg", "keyspace" -> AppConf.getConfig("sunbird.user.report.keyspace"), "cluster" -> "LMSCluster")
  val assessmentAggregatorDBSettings = Map("table" -> "assessment_aggregator", "keyspace" -> AppConf.getConfig("sunbird.user.report.keyspace"), "cluster" -> "LMSCluster")
  val cassandraUrl = "org.apache.spark.sql.cassandra"
  implicit val className: String = "org.sunbird.analytics.audit.ScoreMetricMigrationJob"
  val jobName = "ScoreMetricMigrationJob"

  // $COVERAGE-OFF$ Disabling scoverage for main and execute method
  override def main(config: String)(implicit sc: Option[SparkContext], fc: Option[FrameworkContext]): Unit = {
    JobLogger.init(jobName)
    JobLogger.start(s"$jobName started executing", Option(Map("config" -> config, "model" -> jobName)))

    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](config)
    implicit val spark: SparkSession = openSparkSession(jobConfig)
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()
    spark.setCassandraConf("LMSCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.courses.cluster.host")))
    try {
      val res = CommonUtil.time(migrateData(spark, jobConfig))
      val total_records = res._2.count()
      JobLogger.log(s"Updating the $total_records records in the cassandra table", None, INFO)
      updatedTable(res._2, userActivityAggDBSettings)
      JobLogger.end(s"$jobName completed execution", "SUCCESS", Option(Map("timeTaken" -> res._1, "totalRecordsUpdated" -> total_records)))
    } finally {
      frameworkContext.closeContext()
      spark.close()
    }
  }

  // $COVERAGE-ON$

  /**
   * Get the migrateable dataframe by joining assessment_aggregator to user_activity_agg records
   *
   * Configs:
   * batchId: List of batchids to migrate data for specific batch. if empty list, migrates data for all batches
   * metricsType: type to store in agg_details as identifier of agg_detail
   * forceMerge: option to forcefully replace in exisiting agg_details in user_activity_agg
   *
   * @param session SparkSession
   * @param jobConfig JobConfig
   *
   * @return DataFrame
   */
  def migrateData(session: SparkSession, jobConfig: JobConfig): DataFrame = {
    val batchIds: List[String] = jobConfig.modelParams.get.getOrElse("batchId", List()).asInstanceOf[List[String]]
    val metricsType: String = jobConfig.modelParams.get.getOrElse("metricsType", "attempt_metrics").toString
    val forceMerge: Boolean = jobConfig.modelParams.get.getOrElse("forceMerge", true).asInstanceOf[Boolean]

    val updateAggColumn = udf(mergeAggMapCol())
    val updatedAggLastUpdatedCol = udf(mergeAggLastUpdatedMapCol())
    val flatAggList = udf(mergeAggListValues())
    val flatAggLastUpdatedList = udf(mergeAggLastUpdatedListValues())
    val flatAggDetailList = udf(mergeAggDetailListValues())
    val updatedAggDetailsCol = udf(mergeAggDetailsCol())

    val activityAggDF = fetchActivityData(session, batchIds)
    val assessmentAggDF = getBestScoreAggDetailsDF(fetchAssessmentData(session, batchIds), metricsType)

    val filterDF = activityAggDF.join(assessmentAggDF, activityAggDF.col("activity_id") === assessmentAggDF.col("course_id") &&
      activityAggDF.col("userid") === assessmentAggDF.col("user_id") &&
      activityAggDF.col("context_id") === assessmentAggDF.col("batchid"), joinType = "inner")
      .select("agg", "aggregates", "agg_last_updated", "activity_type", "user_id", "context_id", "activity_id", "total_max_score", "best_score", "content_id", "agg_details", "migrating_agg_details", "attempts_count")

    filterDF.withColumn("aggregates", updateAggColumn(col("agg").cast("map<string, double>"), col("aggregates").cast("map<string, double>"), col("total_max_score").cast(sql.types.DoubleType), col("best_score").cast(sql.types.DoubleType), col("attempts_count").cast(sql.types.DoubleType), col("content_id").cast(sql.types.StringType)))
      .withColumn("agg_details", when(lit(forceMerge), col("migrating_agg_details")).otherwise(updatedAggDetailsCol(col("agg_details"), col("migrating_agg_details"))))
      .withColumn("agg_last_updated", updatedAggLastUpdatedCol(col("agg_last_updated").cast("map<string, long>"), col("content_id").cast(sql.types.StringType)))
      .groupBy("activity_type", "user_id", "context_id", "activity_id")
      .agg(collect_list("aggregates").as("aggregates"), collect_list("agg_last_updated").as("agg_last_updated"), collect_list("agg_details").as("agg_details"))
      .withColumn("aggregates", flatAggList(col("aggregates")))
      .withColumn("agg_last_updated", flatAggLastUpdatedList(col("agg_last_updated")))
      .withColumn("agg_details", flatAggDetailList(col("agg_details")))
      .select("activity_type", "user_id", "context_id", "activity_id", "aggregates", "agg_last_updated", "agg_details")
  }

  def updatedTable(data: DataFrame, tableSettings: Map[String, String]): Unit = {
    data.write.format("org.apache.spark.sql.cassandra").options(tableSettings ++ Map("confirm.truncate" -> "false")).mode(SaveMode.Append).save()
    JobLogger.log(s"Updating the records into the db is completed", None, INFO)
  }

  def fetchActivityData(session: SparkSession, batchIds: List[String]): DataFrame = {
    var activityAggDF = fetchData(session, userActivityAggDBSettings, cassandraUrl, new StructType()).withColumnRenamed("user_id", "userid")
    if (batchIds.nonEmpty) {
      val batchIdsList = batchIds.map(batchId => "cb:" + batchId)
      import session.sqlContext.implicits._
      val batchIdDF = session.sparkContext.parallelize(batchIdsList).toDF("context_id")
      activityAggDF = activityAggDF.join(batchIdDF, Seq("context_id"), "inner");
    }
    activityAggDF
  }

  def fetchAssessmentData(session: SparkSession, batchIds: List[String]): DataFrame = {
    var assessmentAggDF = fetchData(session, assessmentAggregatorDBSettings, cassandraUrl, new StructType())
      .select("batch_id", "course_id", "content_id", "user_id", "total_score", "total_max_score", "attempt_id", "last_attempted_on")
    if (batchIds.nonEmpty) {
      import session.sqlContext.implicits._
      val batchIdDF = session.sparkContext.parallelize(batchIds).toDF("batch_id")
      assessmentAggDF = assessmentAggDF.join(batchIdDF, Seq("batch_id"), "inner");
    }
    assessmentAggDF
  }

  def mergeAggMapCol(): (Map[String, Double], Map[String, Double], Double, Double, Double, String) => Map[String, Double] = (agg: Map[String, Double], aggregates: Map[String, Double], max_score: Double, score: Double, attempts_count: Double, content_id: String) => {
    agg ++ aggregates ++ Map(s"score:$content_id" -> score, s"max_score:$content_id" -> max_score, s"attempts_count:$content_id" -> attempts_count)
  }

  def mergeAggLastUpdatedMapCol(): (Map[String, Long], String) => Map[String, Long] = (aggLastUpdated: Map[String, Long], content_id: String) => {
    Map(s"score:$content_id" -> System.currentTimeMillis(), s"max_score:$content_id" -> System.currentTimeMillis(), s"attempts_count:$content_id" -> System.currentTimeMillis()) ++ aggLastUpdated.map(x => Map(x._1 -> new Date(x._2 * 1000).getTime)).flatten.toMap
  }

  def mergeAggListValues(): Seq[Map[String, Double]] => Map[String, Double] = (aggregation: Seq[Map[String, Double]]) => {
    aggregation.toList.flatten.toMap
  }

  def mergeAggLastUpdatedListValues(): Seq[Map[String, Long]] => Map[String, Long] = (aggregation: Seq[Map[String, Long]]) => {
    aggregation.toList.flatten.toMap
  }

  def mergeAggDetailListValues(): Seq[Seq[String]] => List[String] = (aggregation: Seq[Seq[String]]) => {
    aggregation.toList.flatten
  }

  def mergeAggDetailsCol(): (Seq[String], List[String]) => List[String] = (aggDetails: Seq[String], newAggDetails: Seq[String]) => {
    val filteredAggDetails = newAggDetails.foldLeft(List[String]())((finalResult, aggDetail: String) => {
      val aggDetailMap = JSONUtils.deserialize[Map[String, AnyRef]](aggDetail)
      val existingAgg = aggDetails.filter(row => {
        aggDetailMap.get("attempt_id").get == JSONUtils.deserialize[Map[String, AnyRef]](row).get("attempt_id").get
      })
      if (existingAgg.isEmpty) {
        finalResult :+ aggDetail
      } else finalResult
    })

    aggDetails.toList ++ filteredAggDetails
  }



  /**
   * Get the best score out of the attempts and accumulates all atttempts detail in one list
   * @param assessmentDF AssessmentAggregator records DF
   * @param metrics_type
   *
   * @return DataFrame
   * Example Output:
   * +--------+-------------------------+-----------------------+----------+---------------+--------------+-----------------------------------------------------------------------------------------------------------------------------+------------+
   * |user_id |course_id                |content_id             |best_score|total_max_score|attempts_count|migrating_agg_details                                                                                                        |batchid     |
   * +--------+-------------------------+-----------------------+----------+---------------+--------------+-----------------------------------------------------------------------------------------------------------------------------+------------+
   * |user-012|do_1130293726460805121168|do_11307593493010022419|10.0      |15.0           |1             |[{"max_score":15.0,"score":10.0,"type":"attempt_metrics","attempt_id":"attempat-001","content_id":"do_11307593493010022419"}]|cb:batch-002|
   * +--------+-------------------------+-----------------------+----------+---------------+--------------+-----------------------------------------------------------------------------------------------------------------------------+------------+

   */
  def getBestScoreAggDetailsDF(assessmentDF: DataFrame, metrics_type: String): DataFrame = {
    assessmentDF.groupBy("user_id", "batch_id", "course_id", "content_id").agg(
      max("total_score").as("best_score"),
      first("total_max_score").as("total_max_score"),
      count(col("attempt_id")).as("attempts_count"),
      collect_list(to_json(struct(
        col("total_max_score").as("max_score"),
        col("total_score").as("score"),
        lit(metrics_type).as("type"),
        col("attempt_id"),
        col("content_id"),
        col("last_attempted_on").cast(IntegerType).as("attempted_on")
      ))).as("migrating_agg_details")
    ).withColumn("batchid", concat(lit("cb:"), col("batch_id"))).drop("batch_id")
  }
}
