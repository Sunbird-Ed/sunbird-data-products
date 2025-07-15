
package org.sunbird.analytics.job.report

import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra.CassandraSparkSessionFunctions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig}
import org.ekstep.analytics.util.Constants
import org.joda.time.DateTimeZone
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.sunbird.analytics.exhaust.collection.UDFUtils

import java.util.Properties


case class UserCols(userid: String, orgname: Option[String] = Option(""), firstname: Option[String] = Option(""), lastname: Option[String] = Option(""), email: Option[String] = Option(""),
                    phone: Option[String] = Option(""), rootorgid: String,
                    usertype: Option[String] = Option(""), profileConfig: Option[String] = None, createddate: Option[String] = Option(""))

// Move these case classes to top-level (object scope) for Jackson compatibility
case class CourseInfo(identifier: String, name: String, code: String)
case class SearchResult(content: List[CourseInfo])
case class Response(result: SearchResult)


object UserSummaryReport extends IJob with BaseReportsJob {
  val cassandraUrl = "org.apache.spark.sql.cassandra"
  private val redisFormat = "org.apache.spark.sql.redis";
  private val userCacheDBSettings = Map("table" -> "user", "infer.schema" -> "true", "key.column" -> "userid")
  private val userEnrolmentDBSettings = Map("table" -> "user_enrolments", "keyspace" -> AppConf.getConfig("sunbird.user.report.keyspace"), "cluster" -> "ReportCluster");
  private val encryptedFields = Array("email", "phone");
  private val reportCols = Seq("userid", "firstname", "lastname", "username", "email", "usertype", "cin", "fmpsid", "province", "designation", "orgname", "createddate", "num_courses_enrolled", "num_courses_started", "num_courses_completed", "course_metrics")


  val connProperties: Properties = CommonUtil.getPostgresConnectionProps()
  val db: String = AppConf.getConfig("postgres.db")
  val url: String = AppConf.getConfig("postgres.url") + s"$db"
  val requestsTable: String = "user_summary_report"


  implicit val className: String = "org.sunbird.analytics.job.report.UserSummaryReport"
  val jobName = "UserSummaryReport"

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
      val reportData = res._2
      saveToBlob(reportData, jobConfig) // Saving report to blob storage
      saveToPostgres(reportData)
      reportData.unpersist()
    } finally {
      frameworkContext.closeContext()
      spark.close()
    }
  }

  def getUserCacheColumns(): Seq[String] = {
    Seq("userid", "firstname", "lastname", "email", "orgname", "rootorgid", "usertype", "username", "cin", "fmpsid", "province", "createddate", "designation")
  }

  def getUserEnrolromentColumns(): Seq[String] = {
    Seq("userid", "courseid", "batchid", "active", "completedon", "completionpercentage", "enrolled_date", "datetime", "enrolleddate", "progress", "status")
  }

  // $COVERAGE-OFF$ Disabling scoverage for main and execute method
  def init()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig) {
    //spark.setCassandraConf("UserCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.user.cluster.host")))
    spark.setCassandraConf("LMSCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.courses.cluster.host")))
    //spark.setCassandraConf("ContentCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.content.cluster.host")))
    spark.setCassandraConf("ReportCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.report.cluster.host")))
  }

  // $COVERAGE-ON$
  def getUserCacheDF(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame): DataFrame = {
    val cols = getUserCacheColumns()
    val schema = Encoders.product[UserCols].schema
    val df = fetchData(spark, userCacheDBSettings, redisFormat, schema)
      .withColumn("username", concat_ws(" ", col("firstname"), col("lastname")))
      .withColumn("cin", UDFUtils.extractCIN(col("profileConfig")))
      .withColumn("fmpsid", UDFUtils.extractFMPSID(col("profileConfig")))
      .withColumn("province", UDFUtils.extractProvince(col("profileConfig")))
      .withColumn("designation", UDFUtils.extractDesignation(col("profileConfig")))
    val selectedDF = df.select(cols.head, cols.tail: _*)
      .repartition(AppConf.getConfig("exhaust.user.parallelism").toInt, col("userid"))
    selectedDF.persist()
    selectedDF

  }

  def getUserEnrollment(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame): DataFrame = {
    val cols = getUserEnrolromentColumns()
    val df = fetchData(spark, userEnrolmentDBSettings, cassandraUrl, new StructType())
      .filter(lower(col("active")).equalTo("true"))
      .withColumn("enrolleddate", UDFUtils.getLatestValue(col("enrolled_date"), col("enrolleddate")))
    df.select(cols.head, cols.tail: _*)
      .repartition(AppConf.getConfig("exhaust.user.parallelism").toInt, col("userid"))
  }

  def decryptUserInfo(userDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val schema = userDF.schema
    val decryptFields = schema.fields.filter(field => encryptedFields.contains(field.name))
    val resultDF = decryptFields.foldLeft(userDF) { (df, field) =>
      df.withColumn(field.name, UDFUtils.toDecrypt(col(field.name)))
    }
    resultDF
  }

  def prepareReport(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame)(implicit fc: FrameworkContext, config: JobConfig): DataFrame = {
    implicit val sparkSession: SparkSession = spark
    val userEnrolmentDF = getUserEnrollment(spark, fetchData)
    val userCachedDF = getUserCacheDF(spark, fetchData)
    // Join user cache and enrolment
    val userJoinedDF = userCachedDF.join(userEnrolmentDF, Seq("userid"), "inner")

    // Compute metrics per user
    val userCourseAggDF = userJoinedDF.groupBy("userid")
      .agg(
        count(when(col("enrolleddate").isNotNull, true)).as("num_courses_enrolled"),
        count(when((col("progress") > 0 || col("status") === 1) && col("enrolleddate").isNotNull, true)).as("num_courses_started"),
        count(when((col("status") === 2) && col("enrolleddate").isNotNull, true)).as("num_courses_completed"),
        collect_set(when(col("enrolleddate").isNotNull, col("courseid"))).as("courses_enrolled"),
        collect_set(when((col("progress") > 0 || col("status") === 1) && col("enrolleddate").isNotNull, col("courseid"))).as("courses_started"),
        collect_set(when((col("status") === 2) && col("enrolleddate").isNotNull, col("courseid"))).as("courses_completed")
      )
    // Join back to user info for reporting
    val userSummaryDF = userCachedDF.join(userCourseAggDF, Seq("userid"), "left")
      .na.fill(0, Seq("num_courses_enrolled", "num_courses_started", "num_courses_completed"))
    val decryptedSummary = decryptUserInfo(userSummaryDF)

    // Step 1: Collect all unique course IDs from the DataFrame
    val allCourseIds = decryptedSummary
      .select(explode(flatten(array(col("courses_enrolled"), col("courses_started"), col("courses_completed")))))
      .distinct()
      .rdd.map(r => r.getString(0)).collect().toList

    // Step 2: Fetch course details in a single batch
    val courseDetailsMap = getCourseDetails(allCourseIds)
    val broadcastedCourseMap = spark.sparkContext.broadcast(courseDetailsMap)

    // Step 3: Create a UDF that uses the broadcasted map for enrichment
    val enrichCoursesUDF = udf((courseIds: Seq[String]) => {
      if (courseIds == null) null
      else {
        val courseMap = broadcastedCourseMap.value
        courseIds.map { id =>
          val (code, name) = courseMap.getOrElse(id, ("", ""))
          Map("course_id" -> id, "name" -> name, "code" -> code)
        }
      }
    })

    val withEnriched = decryptedSummary
      .withColumn("courses_enrolled", enrichCoursesUDF(col("courses_enrolled")))
      .withColumn("courses_started", enrichCoursesUDF(col("courses_started")))
      .withColumn("courses_completed", enrichCoursesUDF(col("courses_completed")))

    val withCourseMetrics = withEnriched.withColumn(
      "course_metrics",
      to_json(struct(
        col("courses_enrolled"),
        col("courses_started"),
        col("courses_completed")
      ))
    )
    val finalDF = withCourseMetrics.select(reportCols.head, reportCols.tail: _*)
    finalDF
  }

  def saveToPostgres(reportData: DataFrame): Unit = {
    import org.apache.spark.sql.functions.current_timestamp
    val reportDataWithUpdate = reportData.withColumn("updated_date", current_timestamp())
    reportDataWithUpdate.write
      .mode("overwrite") // Use "overwrite" for full refresh, or implement upsert logic as needed
      .jdbc(url, requestsTable, connProperties)
  }

  def saveToBlob(reportData: DataFrame, jobConfig: JobConfig): Unit = {
    val modelParams = jobConfig.modelParams.get
    val reportPath: String = modelParams.getOrElse("reportPath", "user-summary-report/").asInstanceOf[String]
    val container = AppConf.getConfig("cloud.container.reports")
    val objectKey = AppConf.getConfig("course.metrics.cloud.objectKey")
    val storageConfig = getStorageConfig(
      container,
      objectKey,
      jobConfig)
    JobLogger.log(s"Uploading reports to blob storage", None, INFO)
    reportData.saveToBlobStore(storageConfig, "csv", s"${reportPath}user-summary-report-${getDate}", Option(Map("header" -> "true")), None)
  }

  def getDate: String = {
    val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
    dateFormat.print(System.currentTimeMillis());
  }

  def getCourseDetails(courseIds: List[String])(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): Map[String, (String, String)] = {
    if (courseIds.isEmpty) return Map.empty

    val apiURL = Constants.COMPOSITE_SEARCH_URL
    val batchSize = 500 // Assuming the API has a limit on the number of identifiers per request
    val courseBatches = courseIds.distinct.grouped(batchSize).toList

    val courseDetails = courseBatches.flatMap { batch =>
      val searchFilter = Map(
        "request" -> Map(
          "filters" -> Map(
            "identifier" -> batch,
            "status" -> List("Live")
          ),
          "fields" -> List("name", "code", "identifier"),
          "limit" -> batch.size
        )
      )
      val request = JSONUtils.serialize(searchFilter)
      try {
        val response = RestUtil.post[Response](apiURL, request)
        if (response != null && response.result != null && response.result.content != null) {
          response.result.content
        } else {
          List.empty[CourseInfo]
        }
      } catch {
        case e: Exception =>
          JobLogger.log("Error fetching course details from API", Option(Map("error" -> e.getMessage)), INFO)
          List.empty[CourseInfo]
      }
    }

    courseDetails.map(c => c.identifier -> (c.code, c.name)).toMap
  }

  // Returns (code, name) as a tuple. Returns ("", "") if not found.
  def getCourseCodeAndName(courseId: List[String])(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): (String, String) = {
    case class CollectionDetails(result: Map[String, AnyRef])
    val apiURL = Constants.COMPOSITE_SEARCH_URL
    val searchFilter = Map(
      "request" -> Map(
        "filters" -> Map(
          "identifier" -> courseId,
          "status" -> List("Live")
        ),
        "fields" -> List("name", "code"),
        "offset" -> null
      )
    )
    val request = JSONUtils.serialize(searchFilter)
    val response = RestUtil.post[CollectionDetails](apiURL, request).result
    val result = response.getOrElse("content", List())
    val codeList = JSONUtils.deserialize[List[Map[String, Any]]](JSONUtils.serialize(result))
    val code = codeList.headOption.flatMap(_.get("code")).map(_.toString).getOrElse("")
    val name = codeList.headOption.flatMap(_.get("name")).map(_.toString).getOrElse("")
    (code, name)
  }


}

