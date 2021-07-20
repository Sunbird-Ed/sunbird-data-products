package org.sunbird.analytics.exhaust.uci

import java.util.concurrent.CompletableFuture
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._
import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.dispatcher.KafkaDispatcher
import org.ekstep.analytics.framework.driver.BatchJobDriver.getMetricJson
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.util.Constants
import org.joda.time.{DateTime, DateTimeZone}
import org.sunbird.analytics.exhaust.{BaseReportsJob, JobRequest, OnDemandExhaustJob}
import java.util.concurrent.atomic.AtomicInteger
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import scala.collection.immutable.List

trait BaseUciExhaustJob extends BaseReportsJob with IJob with OnDemandExhaustJob with Serializable {

  /** START - Job Execution Methods */
  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {

    JobLogger.init(jobName)
    JobLogger.start(s"$jobName started executing - ver3", Option(Map("config" -> config, "model" -> jobName)))

    implicit val jobConfig = JSONUtils.deserialize[JobConfig](config)
    implicit val spark: SparkSession = openSparkSession(jobConfig)
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()
    try {
      val res = CommonUtil.time(execute());
      // generate metric event and push it to kafka topic
      val metrics = List(Map("id" -> "total-requests", "value" -> res._2.totalRequests), Map("id" -> "success-requests", "value" -> res._2.successRequests), Map("id" -> "failed-requests", "value" -> res._2.failedRequests), Map("id" -> "time-taken-secs", "value" -> Double.box(res._1 / 1000).asInstanceOf[AnyRef]))
      val metricEvent = getMetricJson(jobName, Option(new DateTime().toString(CommonUtil.dateFormat)), "SUCCESS", metrics)
      // $COVERAGE-OFF$
      if (AppConf.getConfig("push.metrics.kafka").toBoolean)
        KafkaDispatcher.dispatch(Array(metricEvent), Map("topic" -> AppConf.getConfig("metric.kafka.topic"), "brokerList" -> AppConf.getConfig("metric.kafka.broker")))
      // $COVERAGE-ON$
      JobLogger.end(s"$jobName completed execution", "SUCCESS", Option(Map("timeTaken" -> res._1, "totalRequests" -> res._2.totalRequests, "successRequests" -> res._2.successRequests, "failedRequests" -> res._2.failedRequests)));
    }  catch {
      case ex: Exception =>
        JobLogger.log(ex.getMessage, None, ERROR);
        JobLogger.end(jobName + " execution failed", "FAILED", Option(Map("model" -> jobName, "statusMsg" -> ex.getMessage)));
        // generate metric event and push it to kafka topic in case of failure
        val metricEvent = getMetricJson(jobName, Option(new DateTime().toString(CommonUtil.dateFormat)), "FAILED", List())
        // $COVERAGE-OFF$
        if (AppConf.getConfig("push.metrics.kafka").toBoolean)
          KafkaDispatcher.dispatch(Array(metricEvent), Map("topic" -> AppConf.getConfig("metric.kafka.topic"), "brokerList" -> AppConf.getConfig("metric.kafka.broker")))
      // $COVERAGE-ON$
    } finally {
      frameworkContext.closeContext();
      spark.close()
      cleanUp()
    }

  }

  def execute()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): Metrics = {

    implicit val sc: SparkContext = spark.sparkContext

    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]());
    val batchNumber = modelParams.get("batchNumber")
    val requests = getRequests(jobId(), batchNumber)
    val storageConfig = getStorageConfig(config, AppConf.getConfig("uci.exhaust.store.prefix"))

    val totalRequests = new AtomicInteger(requests.length)
    JobLogger.log("Total Requests are ", Some(Map("jobId" -> jobId(), "totalRequests" -> requests.length)), INFO)

    val result = for (request <- requests) yield {
      val updRequest: JobRequest = {
        try {
          if (validateRequest(request)) {
            val res = processRequest(request, storageConfig)
            JobLogger.log("The Request is processed. Pending zipping", Some(Map("requestId" -> request.request_id, "timeTaken" -> res.execution_time, "remainingRequest" -> totalRequests.getAndDecrement())), INFO)
            res
          } else {
            JobLogger.log("Request should have conversationId", Some(Map("requestId" -> request.request_id, "remainingRequest" -> totalRequests.getAndDecrement())), INFO)
            markRequestAsFailed(request, "Request should have conversationId")
          }
        } catch {
          case ex: Exception =>
            ex.printStackTrace()
            markRequestAsFailed(request, "Invalid request")
        }
      }
      saveRequestAsync(storageConfig, updRequest)(spark.sparkContext.hadoopConfiguration, fc)
    }
    CompletableFuture.allOf(result: _*) // Wait for all the async tasks to complete
    val completedResult = result.map(f => f.join()); // Get the completed job requests
    Metrics(totalRequests = Some(requests.length), failedRequests = Some(completedResult.count(x => x.status.toUpperCase() == "FAILED")), successRequests = Some(completedResult.count(x => x.status.toUpperCase == "SUCCESS")))
  }

  def validateRequest(request: JobRequest): Boolean = {
    val requestData = JSONUtils.deserialize[Map[String, AnyRef]](request.request_data);
    if (requestData.get("conversationId").isEmpty) false else true
  }

  def processRequest(request: JobRequest, storageConfig: StorageConfig)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, config: JobConfig): JobRequest = {

    val requestData = JSONUtils.deserialize[Map[String, AnyRef]](request.request_data);
    val conversationId = requestData.get("conversationId").getOrElse("").asInstanceOf[String]
    // query conversation API to get start & end dates
    val conversationData = getConversationData(conversationId)
    // prepare config with start & end dates
    val queryConf = config.search.queries.get.apply(0)
    val query = Query(queryConf.bucket, queryConf.prefix, Option(conversationData.startDate), Option(conversationData.endDate))
    val fetcherQuery = Fetcher(config.search.`type`, None, Option(Array(query)))
    // Fetch telemetry data
    // To-Do: add filters
    val rdd = DataFetcher.fetchBatchData[V3Event](fetcherQuery);
    // convert rdd to DF
    val dataCount = sc.longAccumulator("UCITelemetryCount")
    val df = getTelemetryDF(rdd, dataCount)

    // call process method from respective exhaust job
    try {
      val res = CommonUtil.time(process(conversationId, request.requested_channel, df));
      val reportDF = res._2
      val files = reportDF.saveToBlobStore(storageConfig, "csv", getFilePath(conversationId), Option(Map("header" -> "true")), None)
      if (reportDF.count() == 0) {
        markRequestAsFailed(request, "No data found")
      } else {
        request.status = "SUCCESS";
        request.download_urls = Option(List(files.head));
        request.execution_time = Option(res._1);
        request.dt_job_completed = Option(System.currentTimeMillis)
        request
      }
    } catch {
      case ex: Exception => ex.printStackTrace();
        markRequestAsFailed(request, s"Request processing failed with ${ex.getMessage}")
    }
  }

  def getConversationData(conversationId: String)(implicit sc: SparkContext): ConversationData = {
    val url = "v1/bot/get"
    val response = RestUtil.get[Map[String, AnyRef]](url + s"/$conversationId")
    val conversationData = response.get("data").getOrElse(Map()).asInstanceOf[Map[String, AnyRef]]
    ConversationData(conversationId, conversationData.get("name").getOrElse("").asInstanceOf[String], conversationData.get("startDate").getOrElse("").asInstanceOf[String], conversationData.get("endDate").getOrElse("").asInstanceOf[String])
  }

  override def openSparkSession(config: JobConfig): SparkSession = {

    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]());
    JobContext.parallelization = CommonUtil.getParallelization(config)
    val sparkSession = CommonUtil.getSparkSession(JobContext.parallelization, config.appName.getOrElse(config.model))
    setReportsStorageConfiguration(config)(sparkSession)
    sparkSession;
  }

  def getTelemetryDF(data: RDD[V3Event], dataCount: LongAccumulator)(implicit spark: SparkSession): DataFrame = {
    spark.sqlContext.read.json(data.map(f => {
      dataCount.add(1)
      JSONUtils.serialize(f)
    }))
  }

  def getFilePath(conversationId: String)(implicit config: JobConfig): String = {
    getReportPath() + conversationId + "_" + getReportKey() + "_" + getDate()
  }

  def getDate(): String = {
    val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
    dateFormat.print(System.currentTimeMillis());
  }

  /** START - Overridable Methods */
  def jobId(): String;
  def jobName(): String;
  def getReportPath(): String;
  def getReportKey(): String;
  def process(conversationId: String, channelId: String, telemetryDF: DataFrame)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame;

  case class ConversationData(id: String, name: String, startDate: String, endDate: String)
  case class Metrics(totalRequests: Option[Int], failedRequests: Option[Int], successRequests: Option[Int])

}
