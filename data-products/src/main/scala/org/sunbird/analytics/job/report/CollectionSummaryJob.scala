package org.sunbird.analytics.job.report

import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra.CassandraSparkSessionFunctions
import org.apache.spark.sql.functions.{when, _}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.{DateTime, DateTimeZone}
import org.sunbird.analytics.util.{CourseUtils, DecryptUtil, UserData}

import scala.collection.immutable.List


case class CollectionBatch(batchId: String, courseId: String, startDate: String, endDate: String)

case class CourseMetrics(processedBatches: Option[Int], failedBatches: Option[Int], successBatches: Option[Int])

case class CollectionBatchResponses(batchId: String, execTime: Long, status: String)

object CollectionSummaryJob extends optional.Application with IJob with BaseReportsJob {
  val cassandraUrl = "org.apache.spark.sql.cassandra"
  private val userCacheDBSettings = Map("table" -> "user", "infer.schema" -> "true", "key.column" -> "userid")
  private val userEnrolmentDBSettings = Map("table" -> "user_enrolments", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster")
  private val courseBatchDBSettings = Map("table" -> "course_batch", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster")

  implicit val className: String = "org.sunbird.analytics.job.report.CollectionSummaryJob"
  val jobName = "CollectionSummaryJob"

  private val columnsOrder = List("Published by", "Batch id", "Collection id", "Collection name", "Batch start date", "Batch end date", "Total Enrolments", "Total Completion",
    "Has certificate", "Total Certificate issued");

  private val columnMapping = Map("batchid" -> "Batch id",
    "publishedBy" -> "Published by",
    "courseid" -> "Collection id", "collectionName" -> "Collection name",
    "startdate" -> "Batch start date",
    "enddate" -> "Batch end date", "enrolledUsersCount" -> "Total enrolments", "completionUserCount" -> "Total completion",
    "isCertified" -> "Has certificate",
    "certificatedIssuedCount" -> "Total Certificate issued"
  )

  // $COVERAGE-OFF$ Disabling scoverage for main and execute method
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
      saveToBlob(res._2) // Saving report to blob stroage
      JobLogger.end(s"$jobName completed execution", "SUCCESS", Option(Map("timeTaken" -> res._1, "totalRecords" -> res._2.count())))
    } finally {
      frameworkContext.closeContext()
      spark.close()
    }
  }

  // $COVERAGE-OFF$ Disabling scoverage for main and execute method
  def init()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig) {
    DecryptUtil.initialise()
    spark.setCassandraConf("UserCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.user.cluster.host")))
    spark.setCassandraConf("LMSCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.courses.cluster.host")))
    spark.setCassandraConf("ContentCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.content.cluster.host")))
  }

  // $COVERAGE-ON$
  def getUserData(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame): DataFrame = {
    val schema = Encoders.product[UserData].schema
    fetchData(spark, userCacheDBSettings, "org.apache.spark.sql.redis", schema)
      .withColumn("username", concat_ws(" ", col("firstname"), col("lastname")))
  }

  def getUserEnrollment(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame): DataFrame = {
    fetchData(spark, userEnrolmentDBSettings, cassandraUrl, new StructType())
      .select(col("batchid"), col("userid"), col("courseid"), col("active")
        , col("completionpercentage"), col("enrolleddate"), col("completedon"), col("status"), col("certificates"), col("issued_certificates"))
  }

  def prepareReport(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame, batchList: List[String])(implicit fc: FrameworkContext, config: JobConfig): DataFrame = {
    implicit val sparkSession: SparkSession = spark
    implicit val sqlContext: SQLContext = spark.sqlContext
    import spark.implicits._
    val userCachedDF = getUserData(spark, fetchData = fetchData).select("userid", "userchannel")
    val processBatches: DataFrame = filterBatches(spark, fetchData, config, batchList)
      .join(getUserEnrollment(spark, fetchData), Seq("batchid", "courseid"), "left_outer")
      .join(userCachedDF, Seq("userid"), "left_outer").drop("completionpercentage", "active")

    val courseIds = processBatches.select(col("courseid")).distinct().collect().map(_ (0)).toList.asInstanceOf[List[String]]
    val courseInfo = CourseUtils.getCourseInfo(courseIds, 500).toDF("framework", "identifier", "name", "channel", "batches", "organisation")
    JobLogger.log(s"Total courseInfo records ${courseInfo.count()}", None, INFO)
    val filteredBatches = processBatches.join(courseInfo, processBatches.col("courseid") === courseInfo.col("identifier"), "inner")
      .select(processBatches.col("*"), courseInfo.col("identifier"), courseInfo.col("channel"), courseInfo.col("name"), courseInfo.col("organisation"))
      .withColumn("collectionName", col("name"))
      .withColumn("publishedBy", concat_ws(", ", col("organisation")))
      .withColumn("isCertified", when(col("certificates").isNotNull && size(col("certificates").cast("array<map<string, string>>")) > 0, "Y")
        .when(col("issued_certificates").isNotNull && size(col("issued_certificates").cast("array<map<string, string>>")) > 0, "Y").otherwise("N"))
    computeValues(filteredBatches)
  }


  def computeValues(transformedDF: DataFrame): DataFrame = {

    val computedDF = transformedDF.groupBy("batchid", "courseid").agg(
      count(when(col("status") === 2, 1)).as("completionUserCount"),
      count(when(col("isCertified") === "Y", 1)).as("certificatedIssuedCount"),
      count(col("userid")).as("enrolledUsersCount")
    )
    transformedDF.dropDuplicates("courseid", "batchid").join(computedDF, Seq("courseid", "batchid"), "left_outer")
      .withColumn("batchid", concat(lit("batch-"), col("batchid")))
      .select("publishedBy", "batchid", "courseid", "collectionName", "startdate", "enddate", "enrolledUsersCount", "completionUserCount", "certificatedIssuedCount", "isCertified")
  }

  def saveToBlob(reportData: DataFrame): Unit = {
    val container = AppConf.getConfig("cloud.container.reports")
    val objectKey = AppConf.getConfig("course.metrics.cloud.objectKey")
    val storageConfig = getStorageConfig(container, objectKey)
    JobLogger.log(s"Uploading reports to blob storage", None, INFO)
    val reportPath = "collection-summary-reports/"
    val fields = reportData.schema.fieldNames
    val colNames = for (e <- fields) yield columnMapping.getOrElse(e, e)
    val dynamicColumns = fields.toList.filter(e => !columnMapping.keySet.contains(e))
    val columnWithOrder = (columnsOrder ::: dynamicColumns).distinct
    reportData.toDF(colNames: _*).select(columnWithOrder.head, columnWithOrder.tail: _*)
      .saveToBlobStore(storageConfig, "csv", s"${reportPath}summary-report-${getDate()}", Option(Map("header" -> "true")), None)
  }

  def getDate(): String = {
    val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
    dateFormat.print(System.currentTimeMillis());
  }

  /**
   * Filtering the batches by job config ("generateForAllBatches", "batchEnrolDate")
   */

  def filterBatches(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame, config: JobConfig, batchList: List[String]): DataFrame = {
    val modelParams = config.modelParams.get
    val startDate = modelParams.getOrElse("batchStartDate", "").asInstanceOf[String]
    val generateForAllBatches = modelParams.getOrElse("generateForAllBatches", true).asInstanceOf[Boolean]

    val courseBatchData = fetchData(spark, courseBatchDBSettings, cassandraUrl, new StructType())
      .select("courseid", "batchid", "enddate", "startdate")
    val filteredBatches = if (batchList.nonEmpty) {
      JobLogger.log("Generating reports only for mentioned batch Id's", None, INFO)
      courseBatchData.filter(batch => batchList.contains(batch.getString(1))) // In this case report all batches which have a start date after the specific date irrespective of whether the batch is live or expired
    } else if (startDate.nonEmpty) {
      JobLogger.log(s"Generating reports only for the batches which are started from $startDate date ", None, INFO)
      courseBatchData.filter(col("startdate").isNotNull && to_date(col("startdate"), "yyyy-MM-dd").geq(lit(startDate))) // Generating a report for only for the batches are started on specific date (enrolledFrom)
    } else if (generateForAllBatches) {
      JobLogger.log(s"Generating reports for all the batches irrespective of whether the batch is live or expired", None, INFO)
      courseBatchData // Irrespective of whether the batch is live or expired
    } else {
      // only report for batches which are ongoing and not expired
      JobLogger.log(s"Generating reports only for batches which are ongoing and not expired", None, INFO)
      val comparisonDate = DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now(DateTimeZone.UTC).minusDays(1))
      courseBatchData.filter(col("enddate").isNull || to_date(col("enddate"), "yyyy-MM-dd").geq(lit(comparisonDate))).toDF()
    }
    JobLogger.log(s"Computing summary agg report for ${filteredBatches.count()}", None, INFO)
    filteredBatches.persist(StorageLevel.MEMORY_ONLY)
  }
}
