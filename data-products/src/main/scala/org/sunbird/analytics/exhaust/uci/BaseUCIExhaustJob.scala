package org.sunbird.analytics.exhaust.uci

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.util.LongAccumulator
import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.dispatcher.KafkaDispatcher
import org.ekstep.analytics.framework.driver.BatchJobDriver.getMetricJson
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.{DateTime, DateTimeZone}
import org.sunbird.analytics.exhaust.{BaseReportsJob, JobRequest, OnDemandExhaustJob}

import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Date, Properties}
import scala.collection.immutable.List

trait BaseUCIExhaustJob extends BaseReportsJob with IJob with OnDemandExhaustJob with Serializable {

  val fushionAuthconnectionProps: Properties = getUCIPostgresConnectionProps(
    AppConf.getConfig("uci.fushionauth.postgres.user"),
    AppConf.getConfig("uci.fushionauth.postgres.pass")
  )
  val fusionAuthURL: String = AppConf.getConfig("uci.fushionauth.postgres.url") + s"${AppConf.getConfig("uci.fushionauth.postgres.db")}"
  val userTable: String = AppConf.getConfig("uci.postgres.table.user")
  val isConsentToShare = true // Default set to True

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
    if (!requestData.contains("conversationId")) false else true
  }

  def getConversationDates(requestData: Map[String, AnyRef], conversationDF: DataFrame): Map[String, String] = {
    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
    val previousDay = fmt.print(DateTime.now().minusDays(1))
    val startDate: String = Option(requestData.getOrElse("startDate", null).asInstanceOf[String])
      .getOrElse(Option(conversationDF.head().getAs[Date]("startDate"))
        .getOrElse(previousDay)).toString
    val endDate = Option(requestData.getOrElse("endDate", null).asInstanceOf[String])
      .getOrElse(Option(conversationDF.head().getAs[Date]("endDate"))
        .getOrElse(previousDay)).toString
    Map("conversationStartDate" -> startDate, "conversationEndDate" -> endDate)

  }

  def processRequest(request: JobRequest, storageConfig: StorageConfig)(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext, config: JobConfig): JobRequest = {

    val requestData = JSONUtils.deserialize[Map[String, AnyRef]](request.request_data)
    val conversationId = requestData.getOrElse("conversationId", "").asInstanceOf[String]
    // query conversation API to get start & end dates
    val conversationDF = getConversationData(conversationId, request.requested_channel)

    if (conversationDF.count() > 0) {
      val telemetryDF = if (!config.search.`type`.equals("none")) {
        fc.inputEventsCount = sc.longAccumulator("InputEventsCount");

        val startDate = getConversationDates(requestData, conversationDF)("conversationStartDate")
        val endDate = getConversationDates(requestData, conversationDF)("conversationEndDate")
        // prepare config with start & end dates
        val queryConf = config.search.queries.get.apply(0)
        val query = Query(queryConf.bucket, queryConf.prefix, Option(startDate), Option(endDate), None, None, None, None, None, queryConf.file)
        val fetcherQuery = Fetcher(config.search.`type`, None, Option(Array(query)))
        // Fetch telemetry data
        val rdd = DataFetcher.fetchBatchData[V3Event](fetcherQuery);
        // apply eid filter using config
        val data = DataFilter.filterAndSort[V3Event](rdd, config.filters, config.sort);
        // apply pdata.id, tenant and conversation id filter
        val pdataId = config.modelParams.getOrElse(Map()).get("botPdataId").getOrElse("")
        val filteredData = data
          .filter{f => (f.context.pdata.getOrElse(V3PData("", None, None)).id.equals(pdataId) && f.context.channel.equals(request.requested_channel))}
          .filter{f =>
            val conversationIds = f.context.cdata.getOrElse(List()).filter(f => f.`type`.equals("Conversation"))
              .map{f => f.id}
            if (conversationIds.contains(conversationId)) true else false
          }
        // convert rdd to DF
        val dataCount = sc.longAccumulator("UCITelemetryCount")
        getTelemetryDF(filteredData, dataCount)
      }
      else spark.emptyDataFrame

      try {
        val res = CommonUtil.time(process(conversationId, telemetryDF, conversationDF));
        val reportDF = res._2
        if (reportDF.count() > 0) {
          val files = reportDF.saveToBlobStore(storageConfig, "csv", getFilePath(conversationId, request.request_id), Option(Map("header" -> "true")), None)
          request.status = "SUCCESS";
          request.download_urls = Option(List(files.head));
          request.execution_time = Option(res._1);
          request.dt_job_completed = Option(System.currentTimeMillis)
          request
        }
        else {
          markRequestAsFailed(request, "No data found")
        }
      } catch {
        case ex: Exception => ex.printStackTrace();
          markRequestAsFailed(request, s"Request processing failed with ${ex.getMessage}")
      }
    }
    else {
      markRequestAsFailed(request, "No data found for conversation in DB")
    }
  }

  def getConversationData(conversationId: String, tenant: String)(implicit spark: SparkSession): DataFrame = {

    val conversationDB: String = AppConf.getConfig("uci.conversation.postgres.db")
    val conversationURL: String = AppConf.getConfig("uci.conversation.postgres.url") + s"$conversationDB"
    val conversationTable: String = AppConf.getConfig("uci.postgres.table.conversation")

    val user = AppConf.getConfig("uci.conversation.postgres.user")
    val pass = AppConf.getConfig("uci.conversation.postgres.pass")
    val connProperties: Properties = getUCIPostgresConnectionProps(user, pass)
    /**
     * Fetch conversation for a specific conversation ID and Tenant
     */
    fetchData(conversationURL, connProperties, conversationTable).select("id", "name", "ownerOrgID", "startDate", "endDate")
      .filter(col("id") === conversationId)
      .filter(col("ownerOrgID") === tenant)
  }

  def getUCIPostgresConnectionProps(user: String, pass: String): Properties = {
    val connProperties = new Properties()
    connProperties.setProperty("driver", "org.postgresql.Driver")
    connProperties.setProperty("user", user)
    connProperties.setProperty("password", pass)
    connProperties
  }


  override def openSparkSession(config: JobConfig): SparkSession = {
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

  def fetchData(url: String, props: Properties, table: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.jdbc(url, table, props)
  }

  def getFilePath(conversationId: String, requestId: String)(implicit config: JobConfig): String = {
    val requestIdPath = if (requestId.nonEmpty) requestId.concat("/") else ""
    getReportPath() + requestIdPath + conversationId + "_" + getReportKey() + "_" + getDate()
  }

  def getDate(): String = {
    val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
    dateFormat.print(System.currentTimeMillis());
  }

  def organizeDF(reportDF: DataFrame, finalColumnMapping: Map[String, String], finalColumnOrder: List[String]): DataFrame = {
    val fields = reportDF.schema.fieldNames
    val colNames = for (e <- fields) yield finalColumnMapping.getOrElse(e, e)
    val dynamicColumns = fields.toList.filter(e => !finalColumnMapping.keySet.contains(e))
    val columnWithOrder = (finalColumnOrder ::: dynamicColumns).distinct
    reportDF.toDF(colNames: _*).select(columnWithOrder.head, columnWithOrder.tail: _*).na.fill("")
  }

  /** START - Overridable Methods */
  def jobId(): String;
  def jobName(): String;
  def getReportPath(): String;
  def getReportKey(): String;
  def process(conversationId: String, telemetryDF: DataFrame, conversationDF: DataFrame)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame;

  case class ConversationData(id: String, name: String, startDate: String, endDate: String)
  case class Metrics(totalRequests: Option[Int], failedRequests: Option[Int], successRequests: Option[Int])

}