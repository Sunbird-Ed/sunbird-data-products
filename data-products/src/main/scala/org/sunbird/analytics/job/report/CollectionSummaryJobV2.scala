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
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.{DateTime, DateTimeZone}
import org.sunbird.analytics.exhaust.collection.UDFUtils
import org.sunbird.analytics.util.{CourseUtils, UserData}

import scala.collection.immutable.List

object CollectionSummaryJobV2 extends optional.Application with IJob with BaseReportsJob {
  val cassandraUrl = "org.apache.spark.sql.cassandra"
  private val userCacheDBSettings = Map("table" -> "user", "infer.schema" -> "true", "key.column" -> "userid")
  private val userEnrolmentDBSettings = Map("table" -> "user_enrolments", "keyspace" -> AppConf.getConfig("sunbird.user.report.keyspace"), "cluster" -> "ReportCluster");
  private val courseBatchDBSettings = Map("table" -> "course_batch", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster")
  private val filterColumns = Seq("contentorg", "batchid", "courseid", "collectionname", "batchname", "startdate", "enddate", "hascertified", "state", "district", "enrolleduserscount", "completionuserscount", "certificateissuedcount", "contentstatus", "keywords", "channel", "timestamp", "orgname", "createdfor", "medium", "subject", "usertype", "usersubtype")
  private val contentFields = Seq("framework", "identifier", "name", "channel", "batches", "organisation", "status", "keywords", "createdFor", "medium", "subject")

  implicit val className: String = "org.sunbird.analytics.job.report.CollectionSummaryJobV2"
  val jobName = "CollectionSummaryJobV2"
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
      saveToBlob(res._2, jobConfig) // Saving report to blob storage
      JobLogger.log(s"Submitting Druid Ingestion Task", None, INFO)
      val ingestionSpecPath: String = jobConfig.modelParams.get.getOrElse("specPath", "").asInstanceOf[String]
      val druidIngestionUrl: String = jobConfig.modelParams.get.getOrElse("druidIngestionUrl", "http://localhost:8081/druid/indexer/v1/task").asInstanceOf[String]
      CourseUtils.submitIngestionTask(druidIngestionUrl, ingestionSpecPath) // Starting the ingestion task
      JobLogger.end(s"$jobName completed execution", "SUCCESS", Option(Map("timeTaken" -> res._1, "totalRecords" -> res._2.count())))
      res._2.unpersist()
    } finally {
      frameworkContext.closeContext()
      spark.close()
    }
  }

  // $COVERAGE-OFF$ Disabling scoverage for main and execute method
  def init()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig) {
    //spark.setCassandraConf("UserCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.user.cluster.host")))
    spark.setCassandraConf("LMSCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.courses.cluster.host")))
    //spark.setCassandraConf("ContentCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.content.cluster.host")))
    spark.setCassandraConf("ReportCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.report.cluster.host")))
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
      .withColumn("startdate", UDFUtils.getLatestValue(col("start_date"), col("startdate")))
      .withColumn("enddate", UDFUtils.getLatestValue(col("end_date"), col("enddate")))
      .select("courseid", "batchid", "enddate", "startdate", "cert_templates" , "name")
      .withColumnRenamed("name", "batchname")
      .withColumn("hascertified", when(col("cert_templates").isNotNull && size(col("cert_templates").cast("map<string, map<string, string>>")) > 0, "Y").otherwise("N"))
  }

  def getUserEnrollment(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame): DataFrame = {
    fetchData(spark, userEnrolmentDBSettings, cassandraUrl, new StructType())
      .filter(lower(col("active")).equalTo("true"))
      .withColumn("isCertified",
        when(col("certificates").isNotNull && size(col("certificates").cast("array<map<string, string>>")) > 0
          || col("issued_certificates").isNotNull && size(col("issued_certificates").cast("array<map<string, string>>")) > 0, "Y").otherwise("N"))
      .withColumn("enrolleddate", UDFUtils.getLatestValue(col("enrolled_date"), col("enrolleddate")))
      .select(col("batchid"), col("userid"), col("courseid"), col("enrolleddate"), col("completedon"), col("isCertified"))
      .persist()
  }

  def getContentMetaData(processBatches: DataFrame, spark: SparkSession)(implicit fc: FrameworkContext, config: JobConfig): DataFrame = {
    import spark.implicits._
    val courseIds = processBatches.select(col("courseid")).distinct().map(f => f.getString(0)).collect.toList
    JobLogger.log(s"Total distinct Course Id's ${courseIds.size}", None, INFO)
    val courseInfo = CourseUtils.getCourseInfo(courseIds, None, config.modelParams.get.getOrElse("batchSize", 50).asInstanceOf[Int], Option(config.modelParams.get.getOrElse("contentStatus", CourseUtils.defaultContentStatus.toList).asInstanceOf[List[String]].toArray), Option(config.modelParams.get.getOrElse("contentFields", CourseUtils.defaultContentFields.toList).asInstanceOf[List[String]].toArray)).toDF(contentFields: _*)
    JobLogger.log(s"Total fetched records from content search ${courseInfo.count()}", None, INFO)
    processBatches.join(courseInfo, processBatches.col("courseid") === courseInfo.col("identifier"), "inner")
      .withColumn("collectionname", col("name"))
      .withColumnRenamed("status", "contentstatus")
      .withColumnRenamed("organisation", "contentorg")
      .withColumnRenamed("createdFor", "createdfor")
  }

  def prepareReport(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame)(implicit fc: FrameworkContext, config: JobConfig): DataFrame = {
    implicit val sparkSession: SparkSession = spark
    implicit val sqlContext: SQLContext = spark.sqlContext
    import spark.implicits._
    val userCachedDF = getUserData(spark, fetchData = fetchData).select("userid", "state", "district", "orgname", "usertype", "usersubtype")
    val processBatches: DataFrame = filterBatches(spark, fetchData, config)
      .join(getUserEnrollment(spark, fetchData), Seq("batchid", "courseid"), "left_outer")
      .join(userCachedDF, Seq("userid"), "inner").persist()
    val searchFilter = config.modelParams.get.get("searchFilter").asInstanceOf[Option[Map[String, AnyRef]]]
    val reportDF: DataFrame = if (null == searchFilter || searchFilter.isEmpty) getContentMetaData(processBatches, spark) else processBatches
    val processedBatches = computeValues(reportDF)
    processedBatches.select(filterColumns.head, filterColumns.tail: _*)
  }

  def computeValues(transformedDF: DataFrame): DataFrame = {
    // Compute completionCount and enrolCount for state
    val partitionDF = transformedDF.groupBy("batchid", "courseid", "state", "district").agg(
      count(when(col("completedon").isNotNull, 1)).as("completionuserscount"),
      count(when(col("isCertified") === "Y", 1)).as("certificateissuedcount"),
      count(col("userid")).as("enrolleduserscount")
    )

    partitionDF.join(transformedDF.drop("isCertified", "status", "state", "district").dropDuplicates("courseid", "batchid"), Seq("courseid", "batchid"), "inner")
      .withColumn("batchid", concat(lit("batch-"), col("batchid")))
      .withColumn("timestamp", lit(System.currentTimeMillis()))
  }

  def saveToBlob(reportData: DataFrame, jobConfig: JobConfig): Unit = {
    val modelParams = jobConfig.modelParams.get
    val reportPath: String = modelParams.getOrElse("reportPath", "collection-summary-reports-v2/").asInstanceOf[String]
    val container = AppConf.getConfig("cloud.container.reports")
    val objectKey = AppConf.getConfig("course.metrics.cloud.objectKey")
    val storageConfig = getStorageConfig(
      container,
      objectKey,
      jobConfig)
    JobLogger.log(s"Uploading reports to blob storage", None, INFO)
    reportData.saveToBlobStore(storageConfig, "json", s"${reportPath}collection-summary-report-${getDate}", Option(Map("header" -> "true")), None)
    reportData.saveToBlobStore(storageConfig, "json", s"${reportPath}collection-summary-report-latest", Option(Map("header" -> "true")), None)
  }

  def getDate: String = {
    val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
    dateFormat.print(System.currentTimeMillis());
  }

  /**
   * Filtering the batches by job config ("generateForAllBatches", "batchEnrolDate")
   */
 def filterBatches(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame, config: JobConfig): DataFrame = {
    import spark.implicits._
    val modelParams = config.modelParams.get
    val startDate = modelParams.getOrElse("batchStartDate", "").asInstanceOf[String]
    val generateForAllBatches = modelParams.getOrElse("generateForAllBatches", false).asInstanceOf[Boolean]
    val searchFilter = modelParams.get("searchFilter").asInstanceOf[Option[Map[String, AnyRef]]];
    val courseBatchData = getCourseBatch(spark, fetchData)
    val filteredBatches = if (null != searchFilter && searchFilter.nonEmpty) {
      JobLogger.log("Generating reports only search query", None, INFO)
      val collectionDF = CourseUtils.getCourseInfo(List(), Some(searchFilter.get), 0, None, None).toDF(contentFields: _*)
        .withColumnRenamed("name", "collectionname")
        .withColumnRenamed("status", "contentstatus")
        .withColumnRenamed("organisation", "contentorg")
        .withColumnRenamed("createdFor", "createdfor")
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
    filteredBatches
  }
}

