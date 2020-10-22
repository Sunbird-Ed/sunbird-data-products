package org.sunbird.analytics.job.report

import java.util.concurrent.atomic.AtomicInteger

import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra.CassandraSparkSessionFunctions
import org.apache.spark.sql.functions.{when, _}
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.sunbird.analytics.util.{CourseBatchInfo, CourseUtils, DecryptUtil, UserData}

import scala.collection.immutable.List



case class CollectionBatch(batchId: String, courseId: String, startDate: String, endDate: String)

case class CourseMetrics(processedBatches: Option[Int], failedBatches: Option[Int], successBatches: Option[Int])

case class CollectionBatchResponses(batchId: String, execTime: Long, status: String)

object CollectionSummaryJob extends optional.Application with IJob with BaseReportsJob {
  val cassandraUrl = "org.apache.spark.sql.cassandra"
  private val userCacheDBSettings = Map("table" -> "user", "infer.schema" -> "true", "key.column" -> "userid")
  private val userEnrolmentDBSettings = Map("table" -> "user_enrolments", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster")
  private val organisationDBSettings = Map("table" -> "organisation", "keyspace" -> "sunbird", "cluster" -> "LMSCluster")
  private val courseBatchDBSettings = Map("table" -> "course_batch", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster")

  implicit val className: String = "org.sunbird.analytics.job.report.CollectionSummaryJob"
  val jobName = "CollectionSummaryJob"

  private val columnsOrder = List("Published by", "Collection id", "Collection name", "Batch start date", "Batch end date", "Total Enrolments", "Total Completion", "Total Enrolment from same org", "Total Completion from same org",
    "Is certified course", "Total Certificate issues", "Average elapsed time to complete the course");

  private val columnMapping = Map("publishedBy" -> "Published by", "collectionId" -> "Collection id", "collectionName" -> "Collection name", "batchStartDate" -> "Batch start date",
    "batchEndDate" -> "Batch end date", "enrolmentCount" -> "Total Enrolments", "completionCount" -> "Total Completion",
    "enrolCountBySameOrg" -> "Total Enrolment from same org", "completionCountBySameOrg" -> "Total Completion from same org",
    "isCertified" -> "Is certified course",
    "certificateIssuesCount" -> "Total Certificate issues",
    "avgElapsedTime" -> "Average elapsed time to complete the course")


  override def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
    JobLogger.init(jobName)
    JobLogger.start(s"$jobName started executing", Option(Map("config" -> config, "model" -> jobName)))
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](config)
    implicit val spark: SparkSession = openSparkSession(jobConfig)
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()
    init()
    val conf = config.split(";")
    val batchIds = if (conf.length > 1) {
      conf(1).split(",").toList
    } else List()
    try {
      val res = CommonUtil.time(prepareReport(spark, fetchData, batchIds))
      JobLogger.end(s"$jobName completed execution", "SUCCESS", Option(Map("timeTaken" -> res._1, "processedBatches" -> res._2.processedBatches, "successBatches" -> res._2.successBatches, "failedBatches" -> res._2.failedBatches)))
    } finally {
      frameworkContext.closeContext()
      spark.close()
    }
  }

  def init()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig) {
    DecryptUtil.initialise()
    spark.setCassandraConf("UserCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.user.cluster.host")))
    spark.setCassandraConf("LMSCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.courses.cluster.host")))
    spark.setCassandraConf("ContentCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.content.cluster.host")))
  }

  def loadData(spark: SparkSession, settings: Map[String, String], url: String, schema: StructType): DataFrame = {
    if (schema.nonEmpty) {
      spark.read.schema(schema).format(url).options(settings).load()
    }
    else {
      spark.read.format(url).options(settings).load()
    }
  }

  def getUserData(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame): DataFrame = {
    val schema = Encoders.product[UserData].schema
    fetchData(spark, userCacheDBSettings, "org.apache.spark.sql.redis", schema)
      .withColumn("username", concat_ws(" ", col("firstname"), col("lastname"))).persist(StorageLevel.MEMORY_ONLY)
  }

  def getUserEnrollment(spark: SparkSession, courseId: String, batchId: String, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame): DataFrame = {
    fetchData(spark, userEnrolmentDBSettings, cassandraUrl, new StructType())
      .where(col("courseid") === courseId && col("batchid") === batchId && lower(col("active")).equalTo("true") && col("enrolleddate").isNotNull)
      .select(col("batchid"), col("userid"), col("courseid"), col("active")
        , col("completionpercentage"), col("enrolleddate"), col("completedon"), col("status"), col("certificates"))
  }

  def getOrganisationDetails(spark: SparkSession, channel: String, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame): DataFrame = {
    fetchData(spark, organisationDBSettings, cassandraUrl, new StructType()).select("channel", "orgname", "id")
      .where(col("channel") === channel)
  }


  def prepareReport(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame, batchList: List[String])(implicit fc: FrameworkContext, config: JobConfig): CourseMetrics = {
    implicit val sparkSession: SparkSession = spark
    implicit val sqlContext: SQLContext = spark.sqlContext
    val res = CommonUtil.time({
      val userDF = getUserData(spark, fetchData = fetchData).select("userid", "orgname", "userchannel")
      (userDF.count(), userDF)
    })
    JobLogger.log("Time to fetch user details", Some(Map("timeTaken" -> res._1, "count" -> res._2._1)), INFO)
    val userCachedDF = res._2._2;

    val encoder = Encoders.product[CollectionBatch];
    val processBatches = filterBatches(spark, fetchData, config, batchList).as[CollectionBatch](encoder).collect().toList
    println("processBatches" + processBatches)
    val processBatchesCount = new AtomicInteger(processBatches.length)
    JobLogger.log("Total Batches to process", Some("totalBatches" -> processBatchesCount.getAndDecrement()), INFO)
    val result: List[CollectionBatchResponses] = for (collectionBatch <- processBatches) yield {
      try {
        val response = processBatch(userCachedDF, collectionBatch, fetchData)
        JobLogger.log("Batch is processed", Some(Map("batchId" -> response.batchId, "courseId" -> collectionBatch.courseId,  "execTime" -> response.execTime, "remainingBatch" -> processBatchesCount.getAndDecrement())), INFO)
        response
      } catch {
        case ex: Exception => {
          ex.printStackTrace()
          JobLogger.log("Batch is failed", Some(Map("batchId" -> collectionBatch.batchId, "courseId" -> collectionBatch.courseId, "remainingBatch" -> processBatchesCount.getAndDecrement(), "errMessage" -> ex.getMessage)), INFO)
          CollectionBatchResponses(batchId = collectionBatch.batchId, 0L, "FAILED")
        }
      }
    }
    CourseMetrics(processedBatches = Some(result.length), successBatches = Some(result.count(x => x.status.toUpperCase() == "SUCCESS")), failedBatches = Some(result.count(x => x.status.toUpperCase() == "SUCCESS")))
  }

  def processBatch(userCacheDF: DataFrame, collectionBatch: CollectionBatch, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame)
                  (implicit spark: SparkSession, config: JobConfig): CollectionBatchResponses = {
    implicit val sparkSession: SparkSession = spark
    implicit val sqlContext: SQLContext = spark.sqlContext
    import spark.implicits._
    val res = CommonUtil.time({
      val filteredContents: List[CourseBatchInfo] = CourseUtils.filterContents(spark, JSONUtils.serialize(Map("request" -> Map("filters" -> Map("identifier" -> collectionBatch.courseId, "status" -> Array("Live", "Unlisted", "Retired")), "fields" -> Array("channel", "identifier", "name")))))
      val channel = filteredContents.map(res => res.channel).headOption.getOrElse("")
      val collectionName = filteredContents.map(res => res.name).headOption.getOrElse("")
      println("channelchannel" + channel)
      val organisationDF = getOrganisationDetails(spark, channel, fetchData).select("orgname", "channel") // Filter by channel and returns channel, orgname, id fields
      println("organisationDF" + organisationDF.show(false))
      val userEnrolment = getUserEnrollment(spark, collectionBatch.courseId, collectionBatch.batchId, fetchData).join(userCacheDF, Seq("userid"), "inner")
        .withColumn("isCertificatedIssued", when(col("certificates").isNotNull && size(col("certificates").cast("array<map<string, string>>")) > 0, "Y").otherwise("N"))
      val publisherName = organisationDF.collect().headOption.getOrElse(Row()).getString(0)
      val completedUsers = userEnrolment.where(col("status") === 2)
      val avgElapsedTime = getAvgElapsedTime(completedUsers)
      val totalCertificatesIssues: Long = userEnrolment.where(col("isCertificatedIssued") === "Y").count()
      val userInfo = Seq(
        (publisherName, collectionName, collectionBatch.batchId, collectionBatch.startDate, collectionBatch.endDate,
          userEnrolment.select("userid").distinct().count(), completedUsers.select("userid").distinct().count(),
          completedUsers.where(completedUsers.col("userchannel") === channel).count(), userEnrolment.where(userEnrolment.col("userchannel") === channel).select("userid").distinct().count(),
          avgElapsedTime, totalCertificatesIssues, if (totalCertificatesIssues > 0) "Y" else "N"
        )
      ).toDF("publishedBy", "collectionName", "collectionId", "batchStartDate", "batchEndDate",
        "enrolmentCount", "completionCount", "completionCountBySameOrg", "enrolCountBySameOrg",
        "avgElapsedTime", "certificateIssuesCount", "isCertified"
      )
      saveToBlob(userInfo, collectionBatch.batchId)
    })
    CollectionBatchResponses(batchId = collectionBatch.batchId, execTime = res._1, status = "SUCCESS")
  }

  def getAvgElapsedTime(usersEnrolmentDF: DataFrame): Long = {
    import org.apache.spark.sql.functions._
    val updatedUser = usersEnrolmentDF
      .withColumn("enrolDate", to_timestamp(col("enrolleddate"), fmt = "yyyy-MM-dd HH:mm:ss"))
      .withColumn("completedDate", to_timestamp(col("completedon"), fmt = "yyyy-MM-dd HH:mm:ss"))
      .withColumn("diffInMinutes", round(col("completedDate").cast(LongType) - col("enrolDate").cast(LongType)) / 60) // Converting to mins
    val diffInMinutes = updatedUser.agg(sum("diffInMinutes").cast("long")).first
    if (diffInMinutes.isNullAt(0)) 0L else diffInMinutes.getLong(0) / updatedUser.select("userid").distinct().count()
  }

  def saveToBlob(reportData: DataFrame, batchId: String): Unit = {
    val container = AppConf.getConfig("cloud.container.reports")
    val objectKey = AppConf.getConfig("course.metrics.cloud.objectKey")
    val storageConfig = getStorageConfig(container, objectKey)
    val reportPath = "collection-summary-reports/"
    val fields = reportData.schema.fieldNames
    val colNames = for (e <- fields) yield columnMapping.getOrElse(e, e)
    val dynamicColumns = fields.toList.filter(e => !columnMapping.keySet.contains(e))
    val columnWithOrder = (columnsOrder ::: dynamicColumns).distinct
    val fin = reportData.toDF(colNames: _*).select(columnWithOrder.head, columnWithOrder.tail: _*)
    println("Final report" + fin.show(false))
    fin.saveToBlobStore(storageConfig, "csv", reportPath + "report-" + batchId, Option(Map("header" -> "true")), None)
  }

  /**
   * Filtering the batches by job config ("generateForAllBatches", "batchEnrolDate")
   */

  def filterBatches(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame, config: JobConfig, batchList: List[String]): DataFrame = {
    val modelParams = config.modelParams.get
    val enrolledFrom = modelParams.getOrElse("batchEnrolDate", "").asInstanceOf[String]
    val generateForAllBatches = modelParams.getOrElse("generateForAllBatches", true).asInstanceOf[Boolean]

    val courseBatchData = fetchData(spark, courseBatchDBSettings, cassandraUrl, new StructType())
      .select("courseid", "batchid", "enddate", "startdate")
    if (batchList.nonEmpty) {
      JobLogger.log("Generating reports only for mentioned batch Id's", None, INFO)
      courseBatchData.filter(batch => batchList.contains(batch.getString(1))) // In this case report all batches which have a start date after the specific date irrespective of whether the batch is live or expired
    } else if (enrolledFrom.nonEmpty) {
      JobLogger.log(s"Generating reports only for the batches which are started from $enrolledFrom date ", None, INFO)
      courseBatchData.filter(col("startdate").isNotNull && to_date(col("startdate"), "yyyy-MM-dd").geq(lit(enrolledFrom))) // Generating a report for only for the batches are started on specific date (enrolledFrom)
    } else if (generateForAllBatches) {
      JobLogger.log(s"Generating reports for all the batches irrespective of whether the batch is live or expired", None, INFO)
      courseBatchData // Irrespective of whether the batch is live or expired
    } else {
      // only report for batches which are ongoing and not expired
      JobLogger.log(s"Generating reports only for batches which are ongoing and not expired", None, INFO)
      val comparisonDate = DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now(DateTimeZone.UTC).minusDays(1))
      courseBatchData.filter(col("enddate").isNull || to_date(col("enddate"), "yyyy-MM-dd").geq(lit(comparisonDate))).toDF()
    }
  }
}
