package org.sunbird.analytics.sourcing

import java.util.Properties

import org.apache.spark.SparkContext
import org.sunbird.analytics.util.CourseUtils
import org.apache.spark.sql.functions.{when, _}
import org.apache.spark.sql._
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.{DruidQueryModel, FrameworkContext, IJob, JobConfig, Level, StorageConfig}
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger, RestUtil}
import org.sunbird.analytics.exhaust.BaseReportsJob
import org.sunbird.cloud.storage.conf.AppConf

case class SourcingContents(primaryCategory: String, createdBy: String, count: Int)

object SourcingSummaryReport extends optional.Application with IJob with BaseReportsJob {

  implicit val className = "org.sunbird.analytics.sourcing.SourcingSummaryReport"
  val jobName: String = "SourcingSummaryReport"
  val db = AppConf.getConfig("postgres.db")
  val url = AppConf.getConfig("postgres.url") + s"$db"
  val connProperties = CommonUtil.getPostgresConnectionProps

  // $COVERAGE-OFF$ Disabling scoverage for main method
  def main(config: String)(implicit sc: Option[SparkContext], fc: Option[FrameworkContext]): Unit = {
    JobLogger.log(s"Started execution - $jobName", None, Level.INFO)
    implicit val jobConfig = JSONUtils.deserialize[JobConfig](config)
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()
    implicit val spark = openSparkSession(jobConfig)

    try {
      val res = CommonUtil.time(execute())
      JobLogger.end(s"$jobName completed execution", "SUCCESS", Option(Map("timeTaken" -> res._1)))
    } finally {
      frameworkContext.closeContext()
      spark.close()
    }
  }

  // $COVERAGE-ON$ Enabling scoverage for all other functions
  def execute()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig) = {
    val programData = loadData(url, AppConf.getConfig("postgres.program.table")).withColumnRenamed("status", "programStatus")
    val nominationData = loadData(url, AppConf.getConfig("postgres.nomination.table"))
    val projectDf = programData.join(nominationData, Seq("program_id"), "outer")
      .select("program_id", "status", "rootorg_id", "user_id")

    process(projectDf)
  }

  def process(projectDf: DataFrame)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig) = {
    val userDf = getUserDetails()
    val resultDf = userDf.join(projectDf, userDf.col("userId") === projectDf.col("user_id"), "outer")
      .withColumnRenamed("user_id", "contributorId")
      .withColumn("reportName", lit("SourcingSummaryReport"))
      .withColumn("timestamp", lit(System.currentTimeMillis()))
    JobLogger.log(s"resultDf count - ${resultDf.count()}", None, Level.INFO)

    val modelParams = config.modelParams.get
    val storageConfig = StorageConfig(AppConf.getConfig("cloud_storage_type"), "reports", "", Option(modelParams.getOrElse("storageKey", "druid_storage_account_key").toString), Option(modelParams.getOrElse("storageSecret", "druid_storage_account_secret").toString))
    resultDf.saveToBlobStore(storageConfig, "json", "sourcing",
      Option(Map("header" -> "true")), Option(List("reportName")))
    submitReportToDruid(modelParams)
  }

  def loadData(url: String, tableName: String)(implicit spark: SparkSession, config: JobConfig): DataFrame = {
    spark.read.jdbc(url, tableName, connProperties)
  }

  def submitReportToDruid(modelParams: Map[String, AnyRef]): Unit = {
    JobLogger.log(s"Submitting Druid Ingestion Task", None, Level.INFO)
    val dataSource = modelParams.getOrElse("dataSource", "sourcing-summary-snapshot").asInstanceOf[String]
    val druidHost = modelParams.getOrElse("druidHost", "http://localhost:8081").asInstanceOf[String]

    val segmentUrl = druidHost + AppConf.getConfig("druid.segment.path") + dataSource + "/segments"
    val deleteSegmentUrl = druidHost + AppConf.getConfig("druid.deletesegment.path") + dataSource + "/segments/"
    val olderSegments = RestUtil.get[List[String]](segmentUrl)
    JobLogger.log(s"olderSegments - $olderSegments", None, Level.INFO)
    //disable older segments
    if (null != olderSegments) {
      olderSegments.foreach(segmentId => {
        val apiUrl = deleteSegmentUrl + segmentId
        RestUtil.delete(apiUrl)
        JobLogger.log(s"Deleted $segmentId", None, Level.INFO)
      })
    }

    val ingestionSpecPath: String = modelParams.getOrElse("specPath", "").asInstanceOf[String]
    val druidIngestionUrl = druidHost + AppConf.getConfig("druid.ingestion.path")
    CourseUtils.submitIngestionTask(druidIngestionUrl, ingestionSpecPath)
    JobLogger.log(s"Druid ingestion completed", None, Level.INFO)
  }

  def getUserDetails()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {
    val openSaberDb = config.modelParams.get.getOrElse("dbName", "opensaberdb")
    val dbUrl = AppConf.getConfig("postgres.url") + openSaberDb

    val vUserData = loadData(dbUrl, AppConf.getConfig("postgres.user.table"))
      .select("userId")
    val vUserOrgData = loadData(dbUrl, AppConf.getConfig("postgres.org.table"))
      .select("userId", "roles")

    val userDf = vUserData.join(vUserOrgData, Seq("userId"), "left")
      .withColumn("userType", when(col("roles").contains("admin"), "Organization")
        .when(col("roles").isNull, "Individual").otherwise("Other"))
      .select("userId", "userType")

    val contentDf = getContents()
    userDf.join(contentDf, userDf.col("userId") === contentDf.col("createdBy"),
      "outer").withColumn("userType", when(col("userType").isNull, "Individual")
      .otherwise(col("userType")))
  }

  def getContents()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {
    implicit val sc = spark.sparkContext
    implicit val sqlContext = new SQLContext(spark.sparkContext)
    import sqlContext.implicits._
    val query = JSONUtils.serialize(config.modelParams.get("druidQuery"))
    val druidQuery = JSONUtils.deserialize[DruidQueryModel](query)
    val druidResponse = DruidDataFetcher.getDruidData(druidQuery)
    druidResponse.map(f => JSONUtils.deserialize[SourcingContents](f)).toDF()
      .withColumnRenamed("count", "totalContributedContent")
  }

}
