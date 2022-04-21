package org.sunbird.analytics.exhaust.collection

import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.dispatcher.KafkaDispatcher
import org.ekstep.analytics.framework.driver.BatchJobDriver.getMetricJson
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig, StorageConfig}
import org.ekstep.analytics.util.Constants
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.{DateTime, DateTimeZone}
import org.sunbird.analytics.exhaust.{BaseReportsJob, JobRequest, OnDemandExhaustJob}
import org.sunbird.analytics.util.DecryptUtil
import java.security.MessageDigest
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.sunbird.analytics.exhaust.collection.ResponseExhaustJobV2.Question

import scala.collection.immutable.List
import scala.collection.mutable.ListBuffer


case class UserData(userid: String, state: Option[String] = Option(""), district: Option[String] = Option(""), orgname: Option[String] = Option(""), firstname: Option[String] = Option(""), lastname: Option[String] = Option(""), email: Option[String] = Option(""),
                    phone: Option[String] = Option(""), rootorgid: String, block: Option[String] = Option(""), schoolname: Option[String] = Option(""), schooludisecode: Option[String] = Option(""), board: Option[String] = Option(""), cluster: Option[String] = Option(""),
                    usertype: Option[String] = Option(""), usersubtype: Option[String] = Option(""))

case class CollectionConfig(batchId: Option[String], searchFilter: Option[Map[String, AnyRef]], batchFilter: Option[List[String]])
case class CollectionBatch(batchId: String, collectionId: String, batchName: String, custodianOrgId: String, requestedOrgId: String, collectionOrgId: String, collectionName: String, userConsent: Option[String] = Some("No"))
case class CollectionBatchResponse(batchId: String, file: String, status: String, statusMsg: String, execTime: Long, fileSize: Long)
case class CollectionDetails(result: Map[String, AnyRef])
case class CollectionInfo(channel: String, identifier: String, name: String, userConsent: Option[String], status: String)
case class Metrics(totalRequests: Option[Int], failedRequests: Option[Int], successRequests: Option[Int], duplicateRequests: Option[Int])
case class ProcessedRequest(channel: String, batchId: String, filePath: String, fileSize: Long)

trait BaseCollectionExhaustJob extends BaseReportsJob with IJob with OnDemandExhaustJob with Serializable {

  private val userCacheDBSettings = Map("table" -> "user", "infer.schema" -> "true", "key.column" -> "userid");
  private val userConsentDBSettings = Map("table" -> "user_consent", "keyspace" -> AppConf.getConfig("sunbird.user.keyspace"), "cluster" -> "UserCluster");
  private val collectionBatchDBSettings = Map("table" -> "course_batch", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster");
  private val systemDBSettings = Map("table" -> "system_settings", "keyspace" -> AppConf.getConfig("sunbird.user.keyspace"), "cluster" -> "UserCluster");
  private val userEnrolmentDBSettings = Map("table" -> "user_enrolments", "keyspace" -> AppConf.getConfig("sunbird.user.report.keyspace"), "cluster" -> "ReportCluster");

  private val redisFormat = "org.apache.spark.sql.redis";
  val cassandraFormat = "org.apache.spark.sql.cassandra";
  val MAX_ERROR_MESSAGE_CHAR = 250

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
      val metrics = List(Map("id" -> "total-requests", "value" -> res._2.totalRequests), Map("id" -> "success-requests", "value" -> res._2.successRequests), Map("id" -> "failed-requests", "value" -> res._2.failedRequests), Map("id" -> "duplicate-requests", "value" -> res._2.duplicateRequests), Map("id" -> "time-taken-secs", "value" -> Double.box(res._1 / 1000).asInstanceOf[AnyRef]))
      val metricEvent = getMetricJson(jobName, Option(new DateTime().toString(CommonUtil.dateFormat)), "SUCCESS", metrics)
      // $COVERAGE-OFF$
      if (AppConf.getConfig("push.metrics.kafka").toBoolean)
        KafkaDispatcher.dispatch(Array(metricEvent), Map("topic" -> AppConf.getConfig("metric.kafka.topic"), "brokerList" -> AppConf.getConfig("metric.kafka.broker")))
      // $COVERAGE-ON$
      JobLogger.end(s"$jobName completed execution", "SUCCESS", Option(Map("timeTaken" -> res._1, "totalRequests" -> res._2.totalRequests, "successRequests" -> res._2.successRequests, "failedRequests" -> res._2.failedRequests, "duplicateRequests" -> res._2.duplicateRequests)))
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
    val result: List[CollectionBatchResponse] = processBatches(userCachedDF, collectionBatches._2, storageConfig, None, None, List.empty);
    result.foreach(f => JobLogger.log("Batch Status", Some(Map("status" -> f.status, "batchId" -> f.batchId, "executionTime" -> f.execTime, "message" -> f.statusMsg, "location" -> f.file)), INFO));
    Metrics(totalRequests = Some(result.length), failedRequests = Some(result.count(x => x.status.toUpperCase() == "FAILED")), successRequests = Some(result.count(x => x.status.toUpperCase() == "SUCCESS")), duplicateRequests = Some(0))
  }

  def executeOnDemand(custodianOrgId: String, userCachedDF: DataFrame)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): Metrics = {
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]());
    val batchNumber = modelParams.get("batchNumber")
    val maxErrorMessageLength: Int = modelParams.getOrElse("maxErrorMessageLength", MAX_ERROR_MESSAGE_CHAR).asInstanceOf[Int]
    val requests = getRequests(jobId(), batchNumber)
    val storageConfig = getStorageConfig(config, AppConf.getConfig("collection.exhaust.store.prefix"))
    val totalRequests = new AtomicInteger(requests.length)
    JobLogger.log("Total Requests are ", Some(Map("jobId" -> jobId(), "totalRequests" -> requests.length)), INFO)

    val dupRequests = getDuplicateRequests(requests)
    val dupRequestsList = dupRequests.map(f => f._2).flatMap(f => f).map(f => f.request_id).toList
    val filteredRequests = requests.filter(f => ! dupRequestsList.contains(f.request_id))
    JobLogger.log("The Request count details", Some(Map("Total Requests" -> requests.length, "filtered Requests" -> filteredRequests.length, "Duplicate Requests" -> dupRequestsList.length)), INFO)

    val requestsCompleted :ListBuffer[ProcessedRequest] = ListBuffer.empty

    val result = for (request <- filteredRequests) yield {
      val updRequest: JobRequest = {
        try {
          val processedCount = if(requestsCompleted.isEmpty) 0 else requestsCompleted.filter(f => f.channel.equals(request.requested_channel)).size
          val processedSize = if(requestsCompleted.isEmpty) 0 else requestsCompleted.filter(f => f.channel.equals(request.requested_channel)).map(f => f.fileSize).sum
          JobLogger.log("Channel details at executeOnDemand", Some(Map("channel" -> request.requested_channel, "file size" -> processedSize, "completed batches" -> processedCount)), INFO)

          if (checkRequestProcessCriteria(processedCount, processedSize)) {
            if (validateRequest(request)) {
              val res = processRequest(request, custodianOrgId, userCachedDF, storageConfig, requestsCompleted)
              requestsCompleted.++=(JSONUtils.deserialize[ListBuffer[ProcessedRequest]](res.processed_batches.getOrElse("[]")))
              JobLogger.log("The Request is processed. Pending zipping", Some(Map("requestId" -> request.request_id, "timeTaken" -> res.execution_time, "remainingRequest" -> totalRequests.getAndDecrement())), INFO)
              res
            } else {
              JobLogger.log("Request should have either of batchId, batchFilter, searchFilter or encrption key", Some(Map("requestId" -> request.request_id, "remainingRequest" -> totalRequests.getAndDecrement())), INFO)
              markRequestAsFailed(request, "Request should have either of batchId, batchFilter, searchFilter or encrption key")
            }
          }
          else {
            markRequestAsSubmitted(request, "[]")
            request
          }
        } catch {
          case ex: Exception =>
            ex.printStackTrace()
            JobLogger.log(s"Failed to Process the Request ${ex.getMessage}", Some(Map("requestId" -> request.request_id)), INFO)
            markRequestAsFailed(request, s"Internal Server Error: ${ex.getMessage.take(maxErrorMessageLength)}")
        }
      }
      // check for duplicates and update with same urls
      if (dupRequests.get(updRequest.request_id).nonEmpty){
        val dupReq = dupRequests.get(updRequest.request_id).get
        val res = for (req <- dupReq) yield {
          val dupUpdReq = markDuplicateRequest(req, updRequest)
          dupUpdReq
        }
        saveRequests(storageConfig, res.toArray)(spark.sparkContext.hadoopConfiguration, fc)
      }
      saveRequestAsync(storageConfig, updRequest)(spark.sparkContext.hadoopConfiguration, fc)
    }
    CompletableFuture.allOf(result: _*) // Wait for all the async tasks to complete
    val completedResult = result.map(f => f.join()); // Get the completed job requests
    Metrics(totalRequests = Some(requests.length), failedRequests = Some(completedResult.count(x => x.status.toUpperCase() == "FAILED")), successRequests = Some(completedResult.count(x => x.status.toUpperCase == "SUCCESS")), duplicateRequests =  Some(dupRequestsList.length))
  }

  def markDuplicateRequest(request: JobRequest, referenceRequest: JobRequest): JobRequest = {
    request.status = referenceRequest.status;
    request.download_urls = referenceRequest.download_urls
    request.execution_time = referenceRequest.execution_time
    request.dt_job_completed = referenceRequest.dt_job_completed
    request.processed_batches = referenceRequest.processed_batches
    request.iteration = referenceRequest.iteration
    request.err_message = referenceRequest.err_message
    request
  }

  def checkRequestProcessCriteria(processedCount: Long, processedSize: Long): Boolean = {
    if (processedCount < AppConf.getConfig("exhaust.batches.limit.per.channel").toLong && processedSize < AppConf.getConfig("exhaust.file.size.limit.per.channel").toLong)
      true
    else false
  }

  def processRequest(request: JobRequest, custodianOrgId: String, userCachedDF: DataFrame, storageConfig: StorageConfig, processedRequests: ListBuffer[ProcessedRequest])(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): JobRequest = {
    val batchLimit: Int = AppConf.getConfig("data_exhaust.batch.limit.per.request").toInt
    val collectionConfig = JSONUtils.deserialize[CollectionConfig](request.request_data);
    val batches = if (collectionConfig.batchId.isDefined) List(collectionConfig.batchId.get) else collectionConfig.batchFilter.getOrElse(List[String]())
    if (batches.length <= batchLimit) {
      val completedBatches :ListBuffer[ProcessedRequest]= if(request.processed_batches.getOrElse("[]").equals("[]")) ListBuffer.empty[ProcessedRequest] else {
        JSONUtils.deserialize[ListBuffer[ProcessedRequest]](request.processed_batches.get)
      }
      markRequestAsProcessing(request)
      val completedBatchIds = completedBatches.map(f=> f.batchId)
      val collectionBatches = getCollectionBatches(collectionConfig.batchId, collectionConfig.batchFilter, collectionConfig.searchFilter, custodianOrgId, request.requested_channel)
      val collectionBatchesData = collectionBatches._2.filter(p=> !completedBatchIds.contains(p.batchId))
      //SB-26292: The request should fail if the course is retired with err_message: The request is made for retired collection
      if(collectionBatches._2.size > 0) {
        val result = CommonUtil.time(processBatches(userCachedDF, collectionBatchesData, storageConfig, Some(request.request_id), Some(request.requested_channel), processedRequests.toList))
        val response = result._2;
        val failedBatches = response.filter(p => p.status.equals("FAILED"))
        val processingBatches= response.filter(p => p.status.equals("PROCESSING"))
        response.filter(p=> p.status.equals("SUCCESS")).foreach(f => completedBatches += ProcessedRequest(request.requested_channel, f.batchId,f.file, f.fileSize))
        if (response.size == 0) {
          markRequestAsFailed(request, "No data found")
        } else if (failedBatches.size > 0) {
          markRequestAsFailed(request, failedBatches.map(f => f.statusMsg).mkString(","), Option(JSONUtils.serialize(completedBatches)))
        } else if(processingBatches.size > 0 ){
          markRequestAsSubmitted(request, JSONUtils.serialize(completedBatches))
        } else {
          request.status = "SUCCESS";
          request.download_urls = Option(completedBatches.map(f => f.filePath).toList);
          request.execution_time = Option(result._1);
          request.dt_job_completed = Option(System.currentTimeMillis)
          request.processed_batches = Option(JSONUtils.serialize(completedBatches))
          request
        }
      } else {
        markRequestAsFailed(request, collectionBatches._1)
      }
    } else {
      markRequestAsFailed(request, s"Number of batches in request exceeded. It should be within $batchLimit")
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

  def getCollectionBatches(batchId: Option[String], batchFilter: Option[List[String]], searchFilter: Option[Map[String, AnyRef]], custodianOrgId: String, requestedOrgId: String)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): (String,List[CollectionBatch]) = {

    val encoder = Encoders.product[CollectionBatch];
    val collectionBatches = getCollectionBatchDF(false);
    if (batchId.isDefined || batchFilter.isDefined) {
      val batches = validateBatches(collectionBatches, batchId, batchFilter)
      if (batches.count() > 0) {
        val collectionIds = batches.select("courseid").dropDuplicates().collect().map(f => f.get(0));
        val collectionDF = validCollection(collectionIds)
        if (collectionDF.count() == 0) { ("The request is made for retired collection", List()) }
        else {
          val joinedDF = batches.join(collectionDF, batches("courseid") === collectionDF("identifier"), "inner");
          val finalDF = joinedDF.withColumn("custodianOrgId", lit(custodianOrgId))
            .withColumn("requestedOrgId", when(lit(requestedOrgId) === "System", col("channel")).otherwise(requestedOrgId))
            .select(col("batchid").as("batchId"), col("courseid").as("collectionId"), col("name").as("batchName"), col("custodianOrgId"), col("requestedOrgId"), col("channel").as("collectionOrgId"), col("collectionName"), col("userConsent"));
          ("Successfully fetched the records", finalDF.as[CollectionBatch](encoder).collect().toList)
        }
      } else ("No data found", List())
    } else if (searchFilter.isDefined) {
      val collectionDF = searchContent(searchFilter.get)
      val joinedDF = collectionBatches.join(collectionDF, collectionBatches("courseid") === collectionDF("identifier"), "inner");
      val finalDF = joinedDF.withColumn("custodianOrgId", lit(custodianOrgId))
        .withColumn("requestedOrgId", when(lit(requestedOrgId) === "System", col("channel")).otherwise(requestedOrgId))
        .select(col("batchid").as("batchId"), col("courseid").as("collectionId"), col("name").as("batchName"), col("custodianOrgId"), col("requestedOrgId"), col("channel").as("collectionOrgId"), col("collectionName"), col("userConsent"));
      ("Successfully fetched the records with given searchFilter", finalDF.as[CollectionBatch](encoder).collect().toList)
    } else {
      ("No data found", List());
    }
  }

  /**
    *
    * @param collectionIds
    *    - Filter the collection ids where status=Retired
    * @return Dataset[Row] of valid collection Id
    */
  def validCollection(collectionIds: Array[Any])(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): Dataset[Row] = {
    val searchContentDF = searchContent(Map("request" -> Map("filters" -> Map("identifier" -> collectionIds, "status" -> Array("Live", "Unlisted", "Retired")), "fields" -> Array("channel", "identifier", "name", "userConsent", "status"))));
    searchContentDF.filter(col("status").notEqual("Retired"))
  }

  /**
    *
    * @param collectionBatches,  batchId, batchFilter
    * If batchFilter is defined
    *    Step 1: Filter the duplictae batches from batchFilter list
    * Common Step
    * Step 2: Validate if the batchid is correct by checking in coursebatch table
    *
    * @return Dataset[Row] of valid batchid
    */
  def validateBatches(collectionBatches: DataFrame, batchId: Option[String], batchFilter: Option[List[String]]): Dataset[Row]  = {
    if (batchId.isDefined) {
      collectionBatches.filter(col("batchid") === batchId.get)
    } else {
      /**
        * Filter out the duplicate batches from batchFilter
        * eg: Input: List["batch-001", "batch-002", "batch-001"]
        * Output: List["batch-001", "batch-002"]
        */
      val distinctBatch = batchFilter.get.distinct
      if (batchFilter.size != distinctBatch.size) JobLogger.log("Duplicate Batches are filtered:: TotalDistinctBatches: " + distinctBatch.size)
      collectionBatches.filter(col("batchid").isin(distinctBatch: _*))
    }
  }

  def processBatches(userCachedDF: DataFrame, collectionBatches: List[CollectionBatch], storageConfig: StorageConfig, requestId: Option[String], requestChannel: Option[String], processedRequests: List[ProcessedRequest] )(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): List[CollectionBatchResponse] = {

    var processedCount = if(processedRequests.isEmpty) 0 else processedRequests.filter(f => f.channel.equals(requestChannel.getOrElse(""))).size
    var processedSize = if(processedRequests.isEmpty) 0 else processedRequests.filter(f => f.channel.equals(requestChannel.getOrElse(""))).map(f => f.fileSize).sum
    JobLogger.log("Channel details at processBatches", Some(Map("channel" -> requestChannel, "file size" -> processedSize, "completed batches" -> processedCount)), INFO)

    var newFileSize: Long = 0
    val batches = filterCollectionBatches(collectionBatches)
    val parallelProcessLimit = AppConf.getConfig("exhaust.parallel.batch.load.limit").toInt
    val parallelBatches = batches.sliding(parallelProcessLimit,parallelProcessLimit).toList
    for(parallelBatch <- parallelBatches) yield {
      val userEnrolmentDf = getUserEnrolmentDF(parallelBatch.map(f => f.batchId), true)
      val batchResponseList= for (batch <- parallelBatch) yield {
        if (checkRequestProcessCriteria(processedCount, processedSize)) {
          val userEnrolmentBatchDF = userEnrolmentDf.where(col("batchid") === batch.batchId && col("courseid") === batch.collectionId)
            .join(userCachedDF, Seq("userid"), "inner")
            .withColumn("collectionName", lit(batch.collectionName))
            .withColumn("batchName", lit(batch.batchName))
            .repartition(AppConf.getConfig("exhaust.user.parallelism").toInt,col("userid"),col("courseid"),col("batchid"))
          val filteredDF = filterUsers(batch, userEnrolmentBatchDF).persist()
          val res = CommonUtil.time(filteredDF.count);
          JobLogger.log("Time to fetch batch enrolment", Some(Map("timeTaken" -> res._1, "count" -> res._2)), INFO)
          try {
            val res = CommonUtil.time(processBatch(filteredDF, batch));
            val reportDF = res._2
            val files = reportDF.saveToBlobStore(storageConfig, "csv", getFilePath(batch.batchId, requestId.getOrElse("")), Option(Map("header" -> "true")), None)
            newFileSize = fc.getHadoopFileUtil().size(files.head, spark.sparkContext.hadoopConfiguration)
            CollectionBatchResponse(batch.batchId, files.head, "SUCCESS", "", res._1, newFileSize);
          } catch {
            case ex: Exception => ex.printStackTrace(); CollectionBatchResponse(batch.batchId, "", "FAILED", ex.getMessage, 0, 0);
          } finally {
            processedCount = processedCount + 1
            processedSize = processedSize + newFileSize
            unpersistDFs();
            filteredDF.unpersist(true)
          }
        }
        else {
          CollectionBatchResponse("", "", "PROCESSING", "", 0, 0);
        }
      }
      userEnrolmentDf.unpersist(true);
      batchResponseList
    }
  }.flatMap(f=>f)

  // returns Map of request_id and list of its duplicate requests
  def getDuplicateRequests(requests: Array[JobRequest]): Map[String, List[JobRequest]] = {
    /*
    reqHashMap: contains hash(request_data, encryption_key, requested_by) as key and list of entire req as value
      sample reqHashMap data
      Map<"hash-1", List<JobRequest1, JobRequest3>, "hash-2", List<JobRequest2>>
    */
    val reqHashMap: scala.collection.mutable.Map[String, List[JobRequest]] = scala.collection.mutable.Map()
    requests.foreach{ req =>
      // get hash
      val key = Array(req.request_data, req.encryption_key.getOrElse(""), req.requested_by).mkString("|")
      val hash = MessageDigest.getInstance("MD5").digest(key.getBytes).map("%02X".format(_)).mkString
      if(reqHashMap.get(hash).isEmpty) reqHashMap.put(hash, List(req))
      else {
        val newList = reqHashMap.get(hash).get ++ List(req)
        reqHashMap.put(hash, newList)
      }
    }
    /*
    step-1: filter reqHashMap - with more than 1 entry in value list which indicates duplicates
      sample filtered map data
      Map<"hash-1", List<JobRequest1, JobRequest3>>
    step-2: transform map to have first request_id as key and remaining req list as value
      sample final map data
      Map<"request_id-1", List<JobRequest3>>
    */
    reqHashMap.toMap.filter(f => f._2.size > 1).map(f => (f._2.head.request_id -> f._2.tail))
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

  def getUserEnrolmentDF(batchIds: List[String], persist: Boolean)(implicit spark: SparkSession): DataFrame = {
    val cols = getEnrolmentColumns();
    implicit val sqlContext = new SQLContext(spark.sparkContext)
    import sqlContext.implicits._
    val userDf = loadData(userEnrolmentDBSettings, cassandraFormat, new StructType())
    val batchDf = spark.sparkContext.parallelize(batchIds).toDF("batchid")
    val df =batchDf.join(userDf,Seq("batchid")).where(lower(col("active")).equalTo("true")
       && (col("enrolleddate").isNotNull || col("enrolled_date").isNotNull))
      .withColumn("enrolleddate", UDFUtils.getLatestValue(col("enrolled_date"), col("enrolleddate")))
      .select(cols.head, cols.tail: _*);

    if (persist) df.persist() else df
  }

  def searchContent(searchFilter: Map[String, AnyRef])(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {
    // TODO: Handle limit and do a recursive search call
    implicit val sqlContext = new SQLContext(spark.sparkContext)
    import sqlContext.implicits._

    val apiURL = Constants.COMPOSITE_SEARCH_URL
    val request = JSONUtils.serialize(searchFilter)
    val response = RestUtil.post[CollectionDetails](apiURL, request).result
    var contentDf = spark.createDataFrame(List[CollectionInfo]()).toDF().withColumnRenamed("name", "collectionName").select("channel", "identifier", "collectionName", "userConsent", "status")

    for ((resultKey: String, results: AnyRef) <- response) {
      if (resultKey.toLowerCase != "count") {
        val contents = JSONUtils.deserialize[List[CollectionInfo]](JSONUtils.serialize(results))
        contentDf = contentDf.unionByName(spark.createDataFrame(contents).withColumnRenamed("name", "collectionName").select("channel", "identifier", "collectionName", "userConsent", "status"))
      }
    }

    contentDf
  }

  def getCollectionBatchDF(persist: Boolean)(implicit spark: SparkSession): DataFrame = {
    val df = loadData(collectionBatchDBSettings, cassandraFormat, new StructType())
      .withColumn("startdate", UDFUtils.getLatestValue(col("start_date"), col("startdate")))
      .withColumn("enddate", UDFUtils.getLatestValue(col("end_date"), col("enddate")))
      .select("courseid", "batchid", "enddate", "startdate", "name", "status")
    if (persist) df.persist() else df
  }

  def getUserCacheDF(cols: Seq[String], persist: Boolean)(implicit spark: SparkSession): DataFrame = {
    val schema = Encoders.product[UserData].schema
    val df = loadData(userCacheDBSettings, redisFormat, schema).withColumn("username", concat_ws(" ", col("firstname"), col("lastname"))).select(cols.head, cols.tail: _*)
      .repartition(AppConf.getConfig("exhaust.user.parallelism").toInt,col("userid"))
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

  def filterAssessmentsFromHierarchy(data: List[Map[String, AnyRef]], assessmentFilters: Map[String, List[String]], prevData: AssessmentData): AssessmentData = {
    if (data.nonEmpty) {
      val assessmentTypes = assessmentFilters("assessmentTypes")
      val questionTypes = assessmentFilters("questionTypes")
      val primaryCatFilter = assessmentFilters("primaryCategories")

      val list = data.map(childNode => {
        // TODO: need to change to primaryCategory after 3.3.0
        val contentType = childNode.getOrElse("contentType", "").asInstanceOf[String]
        val objectType = childNode.getOrElse("objectType", "").asInstanceOf[String]
        val primaryCategory = childNode.getOrElse("primaryCategory", "").asInstanceOf[String]

        val updatedIds = (if (assessmentTypes.contains(contentType) || (questionTypes.contains(objectType) && primaryCatFilter.contains(primaryCategory))) {
          List(childNode.get("identifier").get.asInstanceOf[String])
        } else List()) ::: prevData.assessmentIds
        val updatedAssessmentData = AssessmentData(prevData.courseid, updatedIds)
        val children = childNode.getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]]
        if (null != children && children.nonEmpty) {
          filterAssessmentsFromHierarchy(children, assessmentFilters, updatedAssessmentData)
        } else updatedAssessmentData
      })
      val courseId = list.head.courseid
      val assessmentIds = list.map(x => x.assessmentIds).flatten.distinct
      AssessmentData(courseId, assessmentIds)
    } else prevData
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
      .map(xValue => if (xValue.nonEmpty) xValue else staleValue)
      .getOrElse(staleValue)
  }

  val getLatestValue = udf[String, String, String](getLatestValueFun)

  def convertStringToList: UserDefinedFunction =
    udf { str: String => JSONUtils.deserialize[List[Question]](str) }
}
