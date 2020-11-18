package org.sunbird.analytics.job.report

import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra.CassandraSparkSessionFunctions
import org.apache.spark.sql.functions.{when, _}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.dispatcher.ScriptDispatcher
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.{DateTime, DateTimeZone}
import org.sunbird.analytics.util.{CourseUtils, UserData}

import scala.collection.immutable.List

case class MergeScriptConfig(basePath: String, rollup: Integer,
                             container: String, postContainer: Option[String] = None, files: List[MergeFiles])

case class MergeFiles(deltaPath: String, reportPath: String)
case class CollectionBatch(batchId: String, courseId: String, startDate: String, endDate: String)

case class CourseMetrics(processedBatches: Option[Int], failedBatches: Option[Int], successBatches: Option[Int])

case class CollectionBatchResponses(batchId: String, execTime: Long, status: String)

object CollectionSummaryJob extends optional.Application with IJob with BaseReportsJob {
  val cassandraUrl = "org.apache.spark.sql.cassandra"
  private val userCacheDBSettings = Map("table" -> "user", "infer.schema" -> "true", "key.column" -> "userid")
  private val userEnrolmentDBSettings = Map("table" -> "user_enrolments", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster")
  private val courseBatchDBSettings = Map("table" -> "course_batch", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster")
  private val filterColumns = Seq("publishedBy", "batchid", "courseid", "collectionName", "startdate", "enddate", "hasCertified", "state", "enrolledUsersCountByState", "completionUserCountByState", "certificateIssueCount")

  implicit val className: String = "org.sunbird.analytics.job.report.CollectionSummaryJob"
  val jobName = "CollectionSummaryJob"

  private val columnsOrder = List(
    "Published by", "Batch id", "Collection id",
    "Collection name", "Batch start date", "Batch end date",
    "State", "Total enrolments By State", "Total completion By State",
    "Has certificate", "Total Certificate issued by State");

  private val columnMapping = Map("batchid" -> "Batch id",
    "publishedBy" -> "Published by",
    "courseid" -> "Collection id", "collectionName" -> "Collection name",
    "startdate" -> "Batch start date",
    "enddate" -> "Batch end date",
    "hasCertified" -> "Has certificate",
    "enrolledUsersCountByState" -> "Total enrolments By State",
    "completionUserCountByState" -> "Total completion By State",
    "state" -> "State",
    "certificateIssueCount" -> "Total Certificate issued by State"
  )

  // $COVERAGE-OFF$ Disabling scoverage for main and execute method
  override def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
    JobLogger.init(jobName)
    JobLogger.start(s"$jobName started executing", Option(Map("config" -> config, "model" -> jobName)))
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](config)
    implicit val spark: SparkSession = openSparkSession(jobConfig)
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()
    init()
    try {
      val res = CommonUtil.time(prepareReport(spark, fetchData))
      saveToBlob(res._2, jobConfig) // Saving report to blob stroage
      JobLogger.end(s"$jobName completed execution", "SUCCESS", Option(Map("timeTaken" -> res._1, "totalRecords" -> res._2.count())))
    } finally {
      frameworkContext.closeContext()
      spark.close()
    }
  }

  // $COVERAGE-OFF$ Disabling scoverage for main and execute method
  def init()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig) {
    spark.setCassandraConf("UserCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.user.cluster.host")))
    spark.setCassandraConf("LMSCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.courses.cluster.host")))
    spark.setCassandraConf("ContentCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.content.cluster.host")))
  }

  // $COVERAGE-ON$
  def getUserData(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame): DataFrame = {
    val schema = Encoders.product[UserData].schema
    fetchData(spark, userCacheDBSettings, "org.apache.spark.sql.redis", schema)
      .withColumn("username", concat_ws(" ", col("firstname"), col("lastname")))
      .filter(col("state").isNotNull).filter(col("state") =!= "")
  }

  def getCourseBatch(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame): DataFrame = {
    fetchData(spark, courseBatchDBSettings, cassandraUrl, new StructType())
      .select("courseid", "batchid", "enddate", "startdate", "cert_templates")
      .withColumn("hasCertified", when(col("cert_templates").isNotNull && size(col("cert_templates").cast("map<string, map<string, string>>")) > 0, "Y").otherwise("N"))
  }

  def getUserEnrollment(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame): DataFrame = {
    fetchData(spark, userEnrolmentDBSettings, cassandraUrl, new StructType())
      .filter(lower(col("active")).equalTo("true"))
      .withColumn("isCertified",
        when(col("certificates").isNotNull && size(col("certificates").cast("array<map<string, string>>")) > 0
          || col("issued_certificates").isNotNull && size(col("issued_certificates").cast("array<map<string, string>>")) > 0, "Y").otherwise("N"))
      .select(col("batchid"), col("userid"), col("courseid"), col("enrolleddate"), col("completedon"), col("status"), col("isCertified"))
  }

  def prepareReport(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame)(implicit fc: FrameworkContext, config: JobConfig): DataFrame = {
    implicit val sparkSession: SparkSession = spark
    implicit val sqlContext: SQLContext = spark.sqlContext
    import spark.implicits._
    val userCachedDF = getUserData(spark, fetchData = fetchData).select("userid", "state")
    val processBatches: DataFrame = filterBatches(spark, fetchData, config)
      .join(getUserEnrollment(spark, fetchData), Seq("batchid", "courseid"), "left_outer")
      .join(userCachedDF, Seq("userid"), "inner")

    val processedBatches = computeValues(processBatches)
    val searchFilter = config.modelParams.get.get("searchFilter").asInstanceOf[Option[Map[String, AnyRef]]];
    val reportDF = if (searchFilter.isEmpty) {
      val courseIds = processedBatches.select(col("courseid")).distinct().collect().map(_ (0)).toList.asInstanceOf[List[String]]
      val courseInfo = CourseUtils.getCourseInfo(courseIds, None, config.modelParams.get.getOrElse("maxlimit", 500).asInstanceOf[Int]).toDF("framework", "identifier", "name", "channel", "batches", "organisation")
      JobLogger.log(s"Total courseInfo records ${courseInfo.count()}", None, INFO)
      processedBatches.join(courseInfo, processedBatches.col("courseid") === courseInfo.col("identifier"), "inner")
        .withColumn("collectionName", col("name"))
        .withColumn("publishedBy", concat_ws(", ", col("organisation")))
    } else {
      processedBatches
    }
    reportDF.select(filterColumns.head, filterColumns.tail: _*)
  }

  def computeValues(transformedDF: DataFrame): DataFrame = {
    // Compute completionCount and enrolCount for state
    val statePartitionDF = transformedDF.groupBy("batchid", "courseid", "state").agg(
      count(when(col("completedon").isNotNull, 1)).as("completionUserCountByState"),
      count(when(col("isCertified") === "Y", 1)).as("certificateIssueCount"),
      count(col("userid")).as("enrolledUsersCountByState")
    )
    statePartitionDF.join(transformedDF.drop("isCertified", "status", "state").dropDuplicates("courseid", "batchid"), Seq("courseid", "batchid"), "inner")
      .withColumn("batchid", concat(lit("batch-"), col("batchid")))
  }

  def saveToBlob(reportData: DataFrame, jobConfig: JobConfig): Unit = {
    val modelParams = jobConfig.modelParams.get
    val reportPath: String = modelParams.getOrElse("reportPath", "collection-summary-reports/").asInstanceOf[String]
    val container = AppConf.getConfig("cloud.container.reports")
    val objectKey = AppConf.getConfig("course.metrics.cloud.objectKey")
    val storageConfig = getStorageConfig(container, objectKey)
    JobLogger.log(s"Uploading reports to blob storage", None, INFO)
    val fields = reportData.schema.fieldNames
    val colNames = for (e <- fields) yield columnMapping.getOrElse(e, e)
    val dynamicColumns = fields.toList.filter(e => !columnMapping.keySet.contains(e))
    val columnWithOrder = (modelParams.getOrElse("columns", columnsOrder).asInstanceOf[List[String]] ::: dynamicColumns).distinct
    val finalReportDF = reportData.toDF(colNames: _*).select(columnWithOrder.head, columnWithOrder.tail: _*)
    val keyword = modelParams.getOrElse("keywords", null).asInstanceOf[String] // If the keyword is not present then report name is generating without keyword.
    // Generating both csv and json extension two reports one is with date and another one is without date only -latest.
    finalReportDF.saveToBlobStore(storageConfig, "csv", getReportName(keyword, reportPath, s"summary-report-${getDate}"), Option(Map("header" -> "true")), None)
    finalReportDF.saveToBlobStore(storageConfig, "csv", getReportName(keyword, reportPath, "summary-report-latest"), Option(Map("header" -> "true")), None)

    val mergeScriptConfig = MergeScriptConfig(modelParams.getOrElse("baseScriptPath", "/mount/data/analytics/tmp/").asInstanceOf[String], 0, AppConf.getConfig("cloud.container.reports"), AppConf.getConfig("cloud.container.reports"),
      List(MergeFiles(deltaPath = getReportName(keyword, reportPath, "summary-report-latest"), reportPath = getReportName(keyword, reportPath, "summary-report-latest")),
        MergeFiles(deltaPath = getReportName(keyword, reportPath, s"summary-report-${getDate}"), reportPath = getReportName(keyword, reportPath, s"summary-report-${getDate}"))))
    mergeReport(mergeScriptConfig)
  }

  def mergeReport(mergeConfig: MergeScriptConfig, virtualEnvDir: Option[String] = Option("/mount/venv")): Unit = {
    val mergeConfigStr = JSONUtils.serialize(mergeConfig)
    val mergeReportCommand = Seq("bash", "-c",

      s"source ${virtualEnvDir.get}/bin/activate; " +
        s"dataproducts report_merger --report_config='$mergeConfigStr'")
    JobLogger.log(s"Merge report script command:: $mergeReportCommand", None, INFO)
    val mergeReportExitCode = ScriptDispatcher.dispatch(mergeReportCommand)
    if (mergeReportExitCode == 0) {
      JobLogger.log(s"Merge report script::Success", None, INFO)
    } else {
      JobLogger.log(s"Merge report script failed with exit code $mergeReportExitCode", None, ERROR)
      throw new Exception(s"Merge report script failed with exit code $mergeReportExitCode")
    }
  }

  def getDate: String = {
    val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
    dateFormat.print(System.currentTimeMillis());
  }

  def getReportName(keyword: String, reportPath: String, suffix: String): String = {
    if (null == keyword) {
      s"${reportPath}${suffix}"
    } else {
      s"${reportPath}${keyword}-${suffix}"
    }
  }

  /**
   * Filtering the batches by job config ("generateForAllBatches", "batchEnrolDate")
   */

  def filterBatches(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame, config: JobConfig): DataFrame = {
    import spark.implicits._
    val modelParams = config.modelParams.get
    val startDate = modelParams.getOrElse("batchStartDate", "").asInstanceOf[String]
    val generateForAllBatches = modelParams.getOrElse("generateForAllBatches", true).asInstanceOf[Boolean]
    val searchFilter = modelParams.get("searchFilter").asInstanceOf[Option[Map[String, AnyRef]]];
    val courseBatchData = getCourseBatch(spark, fetchData)
    val filteredBatches = if (searchFilter.nonEmpty) {
      JobLogger.log("Generating reports only search query", None, INFO)
      val collectionDF = CourseUtils.getCourseInfo(List(), Some(searchFilter.get), 0).toDF("framework", "identifier", "name", "channel", "batches", "organisation")
        .withColumnRenamed("name", "collectionName")
        .withColumn("publishedBy", concat_ws(", ", col("organisation")))
      courseBatchData.join(collectionDF, courseBatchData("courseid") === collectionDF("identifier"), "inner")
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
