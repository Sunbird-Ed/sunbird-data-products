package org.sunbird.analytics.job.report

import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.cassandra.CassandraSparkSessionFunctions
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig, Dispatcher, OutputDispatcher}
import org.joda.time.DateTimeZone
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.sunbird.analytics.exhaust.UserCacheSupport

object UserMasterDataIndexerV2 extends IJob with BaseReportsJob with UserCacheSupport {
  private val reportCols: Seq[String] = getUserCacheColumns()

  implicit val className: String = "org.sunbird.analytics.job.report.UserMasterDataIndexerV2"
  val jobName = "UserMasterDataIndexerV2"

  override def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
    JobLogger.init(jobName)
    JobLogger.start(s"$jobName started executing", Option(Map("config" -> config, "model" -> jobName)))
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](config)
    implicit val spark: SparkSession = openSparkSession(jobConfig)
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()
    init()
    try {
      val res = CommonUtil.time(prepareReport(spark, fetchData))
      val reportData = res._2

      // Print record count
      val recordCount = reportData.count()
      println(s"Total records in reportData: $recordCount")

      // Print first 5 records
      println("First 5 records in reportData:")
      reportData.show(5, truncate = false)

      // Optionally publish to Kafka based on configuration
      val modelParams = jobConfig.modelParams.getOrElse(Map[String, AnyRef]())
      val publishToKafka = modelParams.getOrElse("publishToKafka", Boolean.box(false)).asInstanceOf[Boolean]
      if (publishToKafka) {
        val topic = modelParams.getOrElse("topic", "").asInstanceOf[String]
        val brokerList = modelParams.getOrElse("brokerList", "").asInstanceOf[String]
        if (topic.trim.nonEmpty && brokerList.trim.nonEmpty) {
          implicit val scForDispatcher: SparkContext = spark.sparkContext
          val jsonRdd = reportData.toJSON.rdd
          OutputDispatcher.dispatch(Dispatcher("kafka", Map("brokerList" -> brokerList, "topic" -> topic)), jsonRdd)
        } else {
          JobLogger.log("Kafka publish enabled but 'topic' or 'brokerList' is missing; skipping publish", None, INFO)
        }
      }

      reportData.unpersist()
    } finally {
      frameworkContext.closeContext()
      spark.close()
    }
  }

  def init()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig) {
    spark.setCassandraConf("ReportCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.report.cluster.host")))
  }

  def prepareReport(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame)(implicit fc: FrameworkContext, config: JobConfig): DataFrame = {
    implicit val sparkSession: SparkSession = spark
    val userCachedDF = getUserCacheDF(spark, fetchData)
    val decrypted = decryptUserInfo(userCachedDF)
    val finalDF = decrypted.select(reportCols.head, reportCols.tail: _*)
    finalDF
  }

}


