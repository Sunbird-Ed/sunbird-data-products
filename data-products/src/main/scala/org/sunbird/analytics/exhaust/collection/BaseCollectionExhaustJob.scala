package org.sunbird.analytics.exhaust.collection

import java.util.concurrent.atomic.AtomicInteger

import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig}
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.util.Constants
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.sunbird.analytics.exhaust.{BaseReportsJob, JobRequest, OnDemandExhaustJob}
import org.sunbird.analytics.util.DecryptUtil

import scala.collection.immutable.List
import java.util.concurrent.CompletableFuture

import org.ekstep.analytics.framework.StorageConfig
import org.ekstep.analytics.framework.dispatcher.KafkaDispatcher
import org.ekstep.analytics.framework.driver.BatchJobDriver.getMetricJson

case class UserData(userid: String, state: Option[String] = Option(""), district: Option[String] = Option(""), orgname: Option[String] = Option(""), firstname: Option[String] = Option(""), lastname: Option[String] = Option(""), email: Option[String] = Option(""),
                    phone: Option[String] = Option(""), rootorgid: String, block: Option[String] = Option(""), schoolname: Option[String] = Option(""), schooludisecode: Option[String] = Option(""), board: Option[String] = Option(""), cluster: Option[String] = Option(""),
                    usertype: Option[String] = Option(""), usersubtype: Option[String] = Option(""))

case class CollectionConfig(batchId: Option[String], searchFilter: Option[Map[String, AnyRef]], batchFilter: Option[List[String]])
case class CollectionBatch(batchId: String, collectionId: String, batchName: String, custodianOrgId: String, requestedOrgId: String, collectionOrgId: String, collectionName: String, userConsent: Option[String] = Some("No"))
case class CollectionBatchResponse(batchId: String, file: String, status: String, statusMsg: String, execTime: Long)
case class CollectionDetails(result: Result)
case class Result(content: List[CollectionInfo])
case class CollectionInfo(channel: String, identifier: String, name: String, userConsent: Option[String])
case class Metrics(totalRequests: Option[Int], failedRequests: Option[Int], successRequests: Option[Int])


trait BaseCollectionExhaustJob extends BaseReportsJob with IJob with OnDemandExhaustJob with Serializable {

  private val userCacheDBSettings = Map("table" -> "user", "infer.schema" -> "true", "key.column" -> "userid");
  private val userConsentDBSettings = Map("table" -> "user_consent", "keyspace" -> AppConf.getConfig("sunbird.user.keyspace"), "cluster" -> "UserCluster");
  private val collectionBatchDBSettings = Map("table" -> "course_batch", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster");
  private val systemDBSettings = Map("table" -> "system_settings", "keyspace" -> AppConf.getConfig("sunbird.user.keyspace"), "cluster" -> "UserCluster");
  private val userEnrolmentDBSettings = Map("table" -> "user_enrolments", "keyspace" -> AppConf.getConfig("sunbird.user.report.keyspace"), "cluster" -> "ReportCluster");

  private val redisFormat = "org.apache.spark.sql.redis";
  val cassandraFormat = "org.apache.spark.sql.cassandra";

  /** START - Job Execution Methods */
  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
    
    JobLogger.init(jobName)
    JobLogger.start(s"$jobName started executing - ver3", Option(Map("config" -> config, "model" -> jobName)))

    implicit val jobConfig = JSONUtils.deserialize[JobConfig](config)
    implicit val spark: SparkSession = openSparkSession(jobConfig)
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()
    init()
    try {
      val res = CommonUtil.time(execute());
      // generate metric event and push it to kafka topic
      val metrics = List(Map("id" -> "total-requests", "value" -> res._2.totalRequests), Map("id" -> "success-requests", "value" -> res._2.successRequests), Map("id" -> "failed-requests", "value" -> res._2.failedRequests), Map("id" -> "time-taken-secs", "value" -> Double.box(res._1 / 1000).asInstanceOf[AnyRef]))
      val metricEvent = getMetricJson(jobName, Option(new DateTime().toString(CommonUtil.dateFormat)), "SUCCESS", metrics)
      if (AppConf.getConfig("push.metrics.kafka").toBoolean)
        KafkaDispatcher.dispatch(Array(metricEvent), Map("topic" -> AppConf.getConfig("metric.kafka.topic"), "brokerList" -> AppConf.getConfig("metric.kafka.broker")))

      JobLogger.end(s"$jobName completed execution", "SUCCESS", Option(Map("timeTaken" -> res._1, "totalRequests" -> res._2.totalRequests, "successRequests" -> res._2.successRequests, "failedRequests" -> res._2.failedRequests)));
    } finally {
      frameworkContext.closeContext();
      spark.close()
      cleanUp()
    }

  }

  def init()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig) {
    spark.setCassandraConf("UserCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.user.cluster.host")))
    spark.setCassandraConf("LMSCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.courses.cluster.host")))
    spark.setCassandraConf("ContentCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.content.cluster.host")))
    spark.setCassandraConf("ReportCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.report.cluster.host")))
  }

  def execute()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): Metrics = {
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]());
    val mode = modelParams.getOrElse("mode", "OnDemand").asInstanceOf[String];

    val custodianOrgId = getCustodianOrgId();

    val res = CommonUtil.time({
      val userDF = getUserCacheDF(getUserCacheColumns(), true)
      (userDF.count(), userDF)
    })
    JobLogger.log("Time to fetch enrolment details", Some(Map("timeTaken" -> res._1, "count" -> res._2._1)), INFO)
    val userCachedDF = res._2._2;
    mode.toLowerCase() match {
      case "standalone" =>
        executeStandAlone(custodianOrgId, userCachedDF)
      case _ =>
        executeOnDemand(custodianOrgId, userCachedDF);
    }
  }
  
  def executeStandAlone(custodianOrgId: String, userCachedDF: DataFrame)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): Metrics = {
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]());
    val batchId = modelParams.get("batchId").asInstanceOf[Option[String]];
    val batchFilter = modelParams.get("batchFilter").asInstanceOf[Option[List[String]]];
    val searchFilter = modelParams.get("searchFilter").asInstanceOf[Option[Map[String, AnyRef]]];
    val collectionBatches = getCollectionBatches(batchId, batchFilter, searchFilter, custodianOrgId, "System");
    val storageConfig = getStorageConfig(config, AppConf.getConfig("collection.exhaust.store.prefix"))
    val result: List[CollectionBatchResponse] = processBatches(userCachedDF, collectionBatches, storageConfig, None);
    result.foreach(f => JobLogger.log("Batch Status", Some(Map("status" -> f.status, "batchId" -> f.batchId, "executionTime" -> f.execTime, "message" -> f.statusMsg, "location" -> f.file)), INFO));
    Metrics(totalRequests = Some(result.length), failedRequests = Some(result.count(x => x.status.toUpperCase() == "FAILED")), successRequests = Some(result.count(x => x.status.toUpperCase() == "SUCCESS")))
  }

  def executeOnDemand(custodianOrgId: String, userCachedDF: DataFrame)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): Metrics = {
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]());
    val batchNumber = modelParams.get("batchNumber")
    val requests = getRequests(jobId(), batchNumber)
    val storageConfig = getStorageConfig(config, AppConf.getConfig("collection.exhaust.store.prefix"))
    val totalRequests = new AtomicInteger(requests.length)
    JobLogger.log("Total Requests are ", Some(Map("jobId" -> jobId(), "totalRequests" -> requests.length)), INFO)
    val result = for (request <- requests) yield {
      val updRequest: JobRequest = {
        try {
          if (validateRequest(request)) {
            val res = processRequest(request, custodianOrgId, userCachedDF, storageConfig)
            JobLogger.log("The Request is processed. Pending zipping", Some(Map("requestId" -> request.request_id, "timeTaken" -> res.execution_time, "remainingRequest" -> totalRequests.getAndDecrement())), INFO)
            res
          } else {
            JobLogger.log("Invalid Request", Some(Map("requestId" -> request.request_id, "remainingRequest" -> totalRequests.getAndDecrement())), INFO)
            markRequestAsFailed(request, "Invalid request")
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

  def processRequest(request: JobRequest, custodianOrgId: String, userCachedDF: DataFrame, storageConfig: StorageConfig)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): JobRequest = {
    markRequestAsProcessing(request)
    val collectionConfig = JSONUtils.deserialize[CollectionConfig](request.request_data);
    val collectionBatches = getCollectionBatches(collectionConfig.batchId, collectionConfig.batchFilter, collectionConfig.searchFilter, custodianOrgId, request.requested_channel)
    val result = CommonUtil.time(processBatches(userCachedDF, collectionBatches, storageConfig, Some(request.request_id)));
    val response = result._2;
    val failedBatches = response.filter(p => p.status.equals("FAILED"));
    if (response.size == 0) {
      markRequestAsFailed(request, "No data found")
    } else if (failedBatches.size > 0) {
      markRequestAsFailed(request, failedBatches.map(f => f.statusMsg).mkString(","))
    } else {
      request.status = "SUCCESS";
      request.download_urls = Option(response.map(f => f.file));
      request.execution_time = Option(result._1);
      request.dt_job_completed = Option(System.currentTimeMillis)
      request
    }
  }

  def validateRequest(request: JobRequest): Boolean = {
    val collectionConfig = JSONUtils.deserialize[CollectionConfig](request.request_data);
    if (collectionConfig.batchId.isEmpty && (collectionConfig.searchFilter.isEmpty && collectionConfig.batchFilter.isEmpty)) false else true
    // TODO: Check if the requestedBy user role has permission to request for the job
  }

  def markRequestAsProcessing(request: JobRequest) = {
    request.status = "PROCESSING";
    updateStatus(request);
  }

  def getCollectionBatches(batchId: Option[String], batchFilter: Option[List[String]], searchFilter: Option[Map[String, AnyRef]], custodianOrgId: String, requestedOrgId: String)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): List[CollectionBatch] = {

    val encoder = Encoders.product[CollectionBatch];
    val collectionBatches = getCollectionBatchDF(false);
    if (batchId.isDefined || batchFilter.isDefined) {
      val batches = if (batchId.isDefined) collectionBatches.filter(col("batchid") === batchId.get) else collectionBatches.filter(col("batchid").isin(batchFilter.get: _*))
      val collectionIds = batches.select("courseid").dropDuplicates().collect().map(f => f.get(0));
      val collectionDF = searchContent(Map("request" -> Map("filters" -> Map("identifier" -> collectionIds, "status" -> Array("Live", "Unlisted", "Retired")), "fields" -> Array("channel", "identifier", "name", "userConsent"))));
      val joinedDF = batches.join(collectionDF, batches("courseid") === collectionDF("identifier"), "inner");
      val finalDF = joinedDF.withColumn("custodianOrgId", lit(custodianOrgId))
        .withColumn("requestedOrgId", when(lit(requestedOrgId) === "System", col("channel")).otherwise(requestedOrgId))
        .select(col("batchid").as("batchId"), col("courseid").as("collectionId"), col("name").as("batchName"), col("custodianOrgId"), col("requestedOrgId"), col("channel").as("collectionOrgId"), col("collectionName"), col("userConsent"));
      finalDF.as[CollectionBatch](encoder).collect().toList
    } else if (searchFilter.isDefined) {
      val collectionDF = searchContent(searchFilter.get)
      val joinedDF = collectionBatches.join(collectionDF, collectionBatches("courseid") === collectionDF("identifier"), "inner");
      val finalDF = joinedDF.withColumn("custodianOrgId", lit(custodianOrgId))
        .withColumn("requestedOrgId", when(lit(requestedOrgId) === "System", col("channel")).otherwise(requestedOrgId))
        .select(col("batchid").as("batchId"), col("courseid").as("collectionId"), col("name").as("batchName"), col("custodianOrgId"), col("requestedOrgId"), col("channel").as("collectionOrgId"), col("collectionName"), col("userConsent"));
      finalDF.as[CollectionBatch](encoder).collect().toList
    } else {
      List();
    }
  }

  def processBatches(userCachedDF: DataFrame, collectionBatches: List[CollectionBatch], storageConfig: StorageConfig, requestId: Option[String])(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): List[CollectionBatchResponse] = {

    for (batch <- filterCollectionBatches(collectionBatches)) yield {
      
      val userEnrolmentBatchDF = getUserEnrolmentDF(batch.collectionId, batch.batchId, false)
        .join(userCachedDF, Seq("userid"), "inner")
        .withColumn("collectionName", lit(batch.collectionName))
        .withColumn("batchName", lit(batch.batchName));
      val filteredDF = filterUsers(batch, userEnrolmentBatchDF).persist();
      val res = CommonUtil.time(filteredDF.count);
      JobLogger.log("Time to fetch batch enrolment", Some(Map("timeTaken" -> res._1, "count" -> res._2)), INFO)
      try {
        val res = CommonUtil.time(processBatch(filteredDF, batch));
        val reportDF = res._2;
        val files = reportDF.saveToBlobStore(storageConfig, "csv", getFilePath(batch.batchId, requestId.getOrElse("")), Option(Map("header" -> "true")), None);
        CollectionBatchResponse(batch.batchId, files.head, "SUCCESS", "", res._1);
      } catch {
        case ex: Exception => ex.printStackTrace(); CollectionBatchResponse(batch.batchId, "", "FAILED", ex.getMessage, 0);
      } finally {
        unpersistDFs();
        filteredDF.unpersist(true);
      }
    }
  }

  /** END - Job Execution Methods */

  /** START - Overridable Methods */
  def processBatch(userEnrolmentDF: DataFrame, collectionBatch: CollectionBatch)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame;
  def unpersistDFs(){};
  def jobId(): String;
  def jobName(): String;
  def getReportPath(): String;
  def getReportKey(): String;
  def filterCollectionBatches(collectionBatches: List[CollectionBatch]): List[CollectionBatch] = {
    collectionBatches
  }

  def getUserCacheColumns(): Seq[String] = {
    Seq("userid", "state", "district", "rootorgid")
  }

  def getEnrolmentColumns() : Seq[String] = {
    Seq("batchid", "userid", "courseid")
  }
  /** END - Overridable Methods */

  /** START - Utility Methods */

  def getFilePath(batchId: String, requestId: String)(implicit config: JobConfig): String = {
    val requestIdPath = if (requestId.nonEmpty) requestId.concat("/") else ""
    getReportPath() + requestIdPath + batchId + "_" + getReportKey() + "_" + getDate()
  }

  def getDate(): String = {
    val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
    dateFormat.print(System.currentTimeMillis());
  }

  def getCustodianOrgId()(implicit spark: SparkSession): String = {
    loadData(systemDBSettings, cassandraFormat, new StructType())
      .where(col("id") === "custodianOrgId" && col("field") === "custodianOrgId").select(col("value")).select("value").first().getString(0)
  }

  def getUserEnrolmentDF(collectionId: String, batchId: String, persist: Boolean)(implicit spark: SparkSession): DataFrame = {

    val cols = getEnrolmentColumns();
    val df = loadData(userEnrolmentDBSettings, cassandraFormat, new StructType())
      .where(col("batchid") === batchId && col("courseid") === collectionId  && lower(col("active")).equalTo("true") && (col("enrolleddate").isNotNull || col("enrolled_date").isNotNull))
      .withColumn("enrolleddate", UDFUtils.getLatestValue(col("enrolled_date"), col("enrolleddate")))
      .select(cols.head, cols.tail: _*);

    if (persist) df.persist() else df
  }

  def searchContent(searchFilter: Map[String, AnyRef])(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {
    // TODO: Handle limit and do a recursive search call
    val apiURL = Constants.COMPOSITE_SEARCH_URL
    val request = JSONUtils.serialize(searchFilter)
    val response = RestUtil.post[CollectionDetails](apiURL, request).result.content
    spark.createDataFrame(response).withColumnRenamed("name", "collectionName").select("channel", "identifier", "collectionName", "userConsent")
  }

  def getCollectionBatchDF(persist: Boolean)(implicit spark: SparkSession): DataFrame = {
    val df = loadData(collectionBatchDBSettings, cassandraFormat, new StructType())
      .withColumn("startdate", UDFUtils.getLatestValue(col("start_date"), col("startdate")))
      .withColumn("enddate", UDFUtils.getLatestValue(col("end_date"), col("enddate")))
      .select("courseid", "batchid", "enddate", "startdate", "name")
    if (persist) df.persist() else df
  }

  def getUserCacheDF(cols: Seq[String], persist: Boolean)(implicit spark: SparkSession): DataFrame = {
    val schema = Encoders.product[UserData].schema
    val df = loadData(userCacheDBSettings, redisFormat, schema).withColumn("username", concat_ws(" ", col("firstname"), col("lastname"))).select(cols.head, cols.tail: _*);
    if (persist) df.persist() else df
  }

  def filterUsers(collectionBatch: CollectionBatch, reportDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    if (collectionBatch.requestedOrgId.equals(collectionBatch.collectionOrgId)) {
      reportDF
    } else {
      reportDF.where(col("rootOrgId") === collectionBatch.requestedOrgId);
    }
  }

  def getUserConsentDF(collectionBatch: CollectionBatch)(implicit spark: SparkSession): DataFrame = {
    val df = loadData(userConsentDBSettings, cassandraFormat, new StructType());
    df.where(col("object_id") === collectionBatch.collectionId && col("consumer_id") === collectionBatch.requestedOrgId)
      .dropDuplicates("user_id", "object_id", "consumer_id")
      .withColumn("consentflag", when(lower(col("status")) === "active", "true").otherwise("false"))
      .withColumn("last_updated_on", date_format(col("last_updated_on"), "dd/MM/yyyy"))
      .select(col("user_id").as("userid"), col("consentflag"), col("last_updated_on").as("consentprovideddate"));
  }

  def logTime[R](block: => R, message: String): R = {
    val res = CommonUtil.time(block);
    JobLogger.log(message, Some(Map("timeTaken" -> res._1)), INFO)
    res._2
  }

  def organizeDF(reportDF: DataFrame, finalColumnMapping: Map[String, String], finalColumnOrder: List[String]): DataFrame = {
    val fields = reportDF.schema.fieldNames
    val colNames = for (e <- fields) yield finalColumnMapping.getOrElse(e, e)
    val dynamicColumns = fields.toList.filter(e => !finalColumnMapping.keySet.contains(e))
    val columnWithOrder = (finalColumnOrder ::: dynamicColumns).distinct
    reportDF.withColumn("batchid", concat(lit("BatchId_"), col("batchid"))).toDF(colNames: _*).select(columnWithOrder.head, columnWithOrder.tail: _*).na.fill("")
  }
  /** END - Utility Methods */

}

object UDFUtils extends Serializable {
  def toDecryptFun(str: String): String = {
    DecryptUtil.decryptData(str)
  }

  val toDecrypt = udf[String, String](toDecryptFun)

  def fromJSONFun(str: String): Map[String, String] = {
    if (str == null) null else {
      val map = JSONUtils.deserialize[Map[String, String]](str);
      map;
    }
  }

  val fromJSON = udf[Map[String, String], String](fromJSONFun)

  def toJSONFun(array: AnyRef): String = {
    val str = JSONUtils.serialize(array);
    val sanitizedStr = str.replace("\\n", "").replace("\\", "").replace("\"", "'");
    sanitizedStr;
  }

  val toJSON = udf[String, AnyRef](toJSONFun)

  def extractFromArrayStringFun(board: String): String = {
    try {
      val str = JSONUtils.deserialize[AnyRef](board);
      str.asInstanceOf[List[String]].head
    } catch {
      case ex: Exception =>
        board
    }
  }

  val extractFromArrayString = udf[String, String](extractFromArrayStringFun)

  def completionPercentageFunction(statusMap: Map[String, Int], leafNodesCount: Int): Int = {
    try {
      val completedContent = statusMap.filter(p => p._2 == 2).size;
      if(completedContent >= leafNodesCount) 100 else Math.round(((completedContent.toFloat/leafNodesCount) * 100))
    } catch {
      case ex: Exception =>
        ex.printStackTrace();
        0
    }
  }

  val completionPercentage = udf[Int, Map[String, Int], Int](completionPercentageFunction)

  def getLatestValueFun(newValue: String, staleValue: String): String = {
    Option(newValue)
      .map(xValue => if(xValue.nonEmpty) xValue else Option(staleValue).map(yValue => yValue).orNull)
      .getOrElse(Option(staleValue).map(yValue => yValue).orNull)
  }

  val getLatestValue = udf[String, String, String](getLatestValueFun)
}