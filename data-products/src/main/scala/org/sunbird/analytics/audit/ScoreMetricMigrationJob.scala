package org.sunbird.analytics.audit

import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.sql.cassandra.CassandraSparkSessionFunctions
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
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
      val res = CommonUtil.time(migrateData(spark))
      val total_records = res._2.count()
      JobLogger.log(s"Updating the $total_records records in the cassandra table table", None, INFO)
      updatedTable(res._2, userActivityAggDBSettings)
      JobLogger.end(s"$jobName completed execution", "SUCCESS", Option(Map("timeTaken" -> res._1, "totalRecordsUpdated" -> total_records)))
    } finally {
      frameworkContext.closeContext()
      spark.close()
    }
  }

  // $COVERAGE-ON$
  def migrateData(session: SparkSession): DataFrame = {
    val updateAggColumn = udf(mergeAggMapCol())
    val updatedAggLastUpdatedCol = udf(mergeAggLastUpdatedMapCol())
    val flatAggList = udf(mergeAggListValues())
    val flatAggLastUpdatedList = udf(mergeAggLastUpdatedListValues())

    val activityAggDF = fetchActivityData(session)
    val assessmentAggDF = getBestScoreRecordsDF(fetchAssessmentData(session))

    val filterDF = activityAggDF.join(assessmentAggDF, activityAggDF.col("activity_id") === assessmentAggDF.col("course_id") &&
      activityAggDF.col("userid") === assessmentAggDF.col("user_id") &&
      activityAggDF.col("context_id") === assessmentAggDF.col("batchid"), joinType = "inner")
      .select("agg", "agg_last_updated", "activity_type", "user_id", "context_id", "activity_id",  "total_max_score", "total_score", "content_id")

    filterDF.withColumn("agg", updateAggColumn(col("agg").cast("map<string, int>"), col("total_max_score").cast(sql.types.IntegerType), col("total_score").cast(sql.types.IntegerType), col("content_id").cast(sql.types.StringType)))
      .withColumn("agg_last_updated", updatedAggLastUpdatedCol(col("agg_last_updated").cast("map<string, long>"), col("content_id").cast(sql.types.StringType)))
      .groupBy("activity_type", "user_id", "context_id", "activity_id")
      .agg(collect_list("agg").as("agg"), collect_list("agg_last_updated").as("agg_last_updated"))
      .withColumn("agg", flatAggList(col("agg")))
      .withColumn("agg_last_updated", flatAggLastUpdatedList(col("agg_last_updated")))
      .select("activity_type", "user_id", "context_id", "activity_id", "agg", "agg_last_updated")
  }

  def updatedTable(data: DataFrame, tableSettings: Map[String, String]): Unit = {
    data.write.format("org.apache.spark.sql.cassandra").options(tableSettings ++ Map("confirm.truncate" -> "false")).mode(SaveMode.Append).save()
    JobLogger.log(s"Updating the records into the db is completed", None, INFO)
  }

  def fetchActivityData(session: SparkSession): DataFrame = {
    fetchData(session, userActivityAggDBSettings, cassandraUrl, new StructType()).withColumnRenamed("user_id", "userid")
  }

  def fetchAssessmentData(session: SparkSession): DataFrame = {
    fetchData(session, assessmentAggregatorDBSettings, cassandraUrl, new StructType())
      .select("batch_id", "course_id", "content_id", "user_id", "total_score", "total_max_score")
  }

  def mergeAggMapCol(): (Map[String, Int], Int, Int, String) => Map[String, Int] = (agg: Map[String, Int], max_score: Int, score: Int, content_id: String) => {
    agg ++ Map(s"score:$content_id" -> score, s"max_score:$content_id" -> max_score)
  }

  def mergeAggLastUpdatedMapCol(): (Map[String, Long], String) => Map[String, Long] = (aggLastUpdated: Map[String, Long], content_id: String) => {
    aggLastUpdated.map(x => Map(x._1 -> new Date(x._2 * 1000).getTime)).flatten.toMap ++ Map(s"score:$content_id" -> System.currentTimeMillis(), s"max_score:$content_id" -> System.currentTimeMillis())
  }

  def mergeAggListValues(): Seq[Map[String, Int]] => Map[String, Int] = (aggregation: Seq[Map[String, Int]]) => {
    aggregation.toList.flatten.toMap
  }

  def mergeAggLastUpdatedListValues(): Seq[Map[String, Long]] => Map[String, Long] = (aggregation: Seq[Map[String, Long]]) => {
    aggregation.toList.flatten.toMap
  }

  def getBestScoreRecordsDF(assessmentDF: DataFrame): DataFrame = {
    val df = Window.partitionBy("user_id", "batch_id", "course_id", "content_id").orderBy(desc("total_score"))
    assessmentDF.withColumn("rownum", row_number.over(df)).where(col("rownum") === 1).drop("rownum")
      .withColumn("batchid", concat(lit("cb:"), col("batch_id"))).drop("batch_id")
  }

}
