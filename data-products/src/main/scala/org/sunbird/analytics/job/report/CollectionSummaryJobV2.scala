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
import org.sunbird.analytics.util.{CourseUtils, UserData}

import scala.collection.immutable.List

object CollectionSummaryJobV2 extends optional.Application with IJob with BaseReportsJob {
  val cassandraUrl = "org.apache.spark.sql.cassandra"
  private val userCacheDBSettings = Map("table" -> "user", "infer.schema" -> "true", "key.column" -> "userid")
  private val userEnrolmentDBSettings = Map("table" -> "user_enrolments", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster")
  private val courseBatchDBSettings = Map("table" -> "course_batch", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster")
  private val filterColumns = Seq("contentorg", "batchid", "courseid", "collectionname", "startdate", "enddate", "hascertified", "state", "district", "enrolleduserscount", "completionuserscount", "certificateissuedcount", "contentstatus", "keywords", "channel", "timestamp", "orgname")

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
      saveToBlob(res._2, jobConfig) // Saving report to blob stroage
      JobLogger.log(s"Submitting Druid Ingestion Task", None, INFO)
      val ingestionSpecPath: String = jobConfig.modelParams.get.getOrElse("specPath", "").asInstanceOf[String]
      val druidIngestionUrl: String = jobConfig.modelParams.get.getOrElse("druidIngestionUrl", "http://localhost:8081/druid/indexer/v1/task").asInstanceOf[String]
      submitIngestionTask(druidIngestionUrl, ingestionSpecPath) // Starting the ingestion task
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
      .withColumn("hascertified", when(col("cert_templates").isNotNull && size(col("cert_templates").cast("map<string, map<string, string>>")) > 0, "Y").otherwise("N"))
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
    val userCachedDF = getUserData(spark, fetchData = fetchData).select("userid", "state", "district", "orgname")
    val processBatches: DataFrame = filterBatches(spark, fetchData, config)
      .join(getUserEnrollment(spark, fetchData), Seq("batchid", "courseid"), "left_outer")
      .join(userCachedDF, Seq("userid"), "inner")
    val processedBatches = computeValues(processBatches)
    val searchFilter = config.modelParams.get.get("searchFilter").asInstanceOf[Option[Map[String, AnyRef]]];
    val reportDF = if (null == searchFilter || searchFilter.isEmpty) {
      val courseIds = processedBatches.select(col("courseid")).distinct().collect().map(_ (0)).toList.asInstanceOf[List[String]]
      val courseInfo = CourseUtils.getCourseInfo(courseIds, None, config.modelParams.get.getOrElse("maxlimit", 500).asInstanceOf[Int]).toDF("framework", "identifier", "name", "channel", "batches", "organisation", "status", "keywords")
      JobLogger.log(s"Total courseInfo records ${courseInfo.count()}", None, INFO)
      processedBatches.join(courseInfo, processedBatches.col("courseid") === courseInfo.col("identifier"), "inner")
        .withColumn("collectionname", col("name"))
        .withColumnRenamed("status", "contentstatus")
        .withColumnRenamed("organisation", "contentorg")
    } else {
      processedBatches
    }
    reportDF.select(filterColumns.head, filterColumns.tail: _*)
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
    val storageConfig = getStorageConfig(container, objectKey)
    JobLogger.log(s"Uploading reports to blob storage", None, INFO)
    reportData.saveToBlobStore(storageConfig, "json", s"${reportPath}collection-summary-report-${getDate}", Option(Map("header" -> "true")), None)
    reportData.saveToBlobStore(storageConfig, "json", s"${reportPath}collection-summary-report-latest", Option(Map("header" -> "true")), None)
  }

  def getDate: String = {
    val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
    dateFormat.print(System.currentTimeMillis());
  }

  def submitIngestionTask(apiUrl: String, specPath: String): Unit = {
    val source = scala.io.Source.fromFile(specPath)
    val ingestionData = try {
      source.mkString
    } catch {
      case ex: Exception =>
        JobLogger.log(s"Exception Found While reading ingestion spec. ${ex.getMessage}", None, ERROR)
        ex.printStackTrace()
        null
    } finally source.close()
    val response = RestUtil.post[Map[String, String]](apiUrl, ingestionData, None)
    JobLogger.log(s"Ingestion Task Id: $response", None, INFO)
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
      val collectionDF = CourseUtils.getCourseInfo(List(), Some(searchFilter.get), 0).toDF("framework", "identifier", "name", "channel", "batches", "organisation", "status", "keywords")
        .withColumnRenamed("name", "collectionname")
        .withColumnRenamed("status", "contentstatus")
        .withColumnRenamed("organisation", "contentorg")
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

