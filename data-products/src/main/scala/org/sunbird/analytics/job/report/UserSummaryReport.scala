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
import org.sunbird.analytics.exhaust.UserCacheSupport
import org.sunbird.analytics.job.report.UserSummaryReport.fetchData

import java.text.SimpleDateFormat
import java.util.{Properties, TimeZone}


case class UserCols(userid: String, orgname: Option[String] = Option(""), firstname: Option[String] = Option(""), lastname: Option[String] = Option(""), email: Option[String] = Option(""),
                    phone: Option[String] = Option(""), rootorgid: String,
                    usertype: Option[String] = Option(""), profileConfig: Option[String] = None, createddate: Option[String] = Option(""), start_date: Option[String] = Option(""), end_date: Option[String] = Option(""))

// Move these case classes to top-level (object scope) for Jackson compatibility
case class CourseInfo(identifier: String, name: String, code: String)
case class SearchResult(content: List[CourseInfo])
case class Response(result: SearchResult)


object UserSummaryReport extends IJob with BaseReportsJob with UserCacheSupport {
  val cassandraUrl = "org.apache.spark.sql.cassandra"
  private val userEnrolmentDBSettings = Map("table" -> "user_enrolments", "keyspace" -> AppConf.getConfig("sunbird.user.report.keyspace"), "cluster" -> "ReportCluster");
  private val courseBatchDBSettings = Map("table" -> "course_batch", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster")
  private val reportCols = Seq("userid", "firstname", "lastname", "username", "email", "usertype", "cin", "fmpsid", "province", "designation", "training_group", "orgname", "createddate", "num_courses_enrolled", "num_courses_started", "num_courses_completed", "course_metrics")


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

  def getUserEnrolromentColumns(): Seq[String] = {
    Seq("userid", "courseid", "batchid", "active", "completedon", "completionpercentage",
      "enrolled_date", "datetime", "enrolleddate", "progress", "status", "lastcontentaccesstime")
  }

  // $COVERAGE-OFF$ Disabling scoverage for main and execute method
  def init()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig) {
    //spark.setCassandraConf("UserCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.user.cluster.host")))
    spark.setCassandraConf("LMSCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.courses.cluster.host")))
    //spark.setCassandraConf("ContentCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.content.cluster.host")))
    spark.setCassandraConf("ReportCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.report.cluster.host")))
  }

  // $COVERAGE-ON$
  def getUserEnrollment(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame): DataFrame = {
    val cols = getUserEnrolromentColumns()
    val df = fetchData(spark, userEnrolmentDBSettings, cassandraUrl, new StructType())
      .filter(lower(col("active")).equalTo("true"))
      .withColumn("enrolleddate", UDFUtils.getLatestValue(col("enrolled_date"), col("enrolleddate")))
    df.select(cols.head, cols.tail: _*)
      .repartition(AppConf.getConfig("exhaust.user.parallelism").toInt, col("userid"))
  }

  def getCourseBatchDF(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame): DataFrame = {
    fetchData(spark, courseBatchDBSettings, cassandraUrl, new StructType())
      .select("courseid", "batchid", "name", "start_date", "end_date")
  }

  // UDF to calculate time spent between completedon and lastcontentaccesstime
  def calculateTimeSpent(completedOn: String, lastContentAccessTime: String): String = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))

    val outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS'+0000'")
    outputFormat.setTimeZone(TimeZone.getTimeZone("UTC"))

    try {
      (Option(completedOn), Option(lastContentAccessTime)) match {
        case (Some(completed), Some(lastAccess)) if completed.nonEmpty && lastAccess.nonEmpty =>
          // Both values present - calculate difference
          val completedDate = if (completed.contains("T")) {
            val isoFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
            isoFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
            isoFormat.parse(completed)
          } else {
            dateFormat.parse(completed)
          }

          val lastAccessDate = if (lastAccess.contains("T")) {
            val isoFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
            isoFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
            isoFormat.parse(lastAccess)
          } else {
            dateFormat.parse(lastAccess)
          }

          val diffMillis = Math.abs(completedDate.getTime - lastAccessDate.getTime)
          val diffDate = new java.util.Date(diffMillis)
          outputFormat.format(diffDate)

        case (Some(completed), _) if completed.nonEmpty =>
          // Only completed date present
          completed

        case (_, Some(lastAccess)) if lastAccess.nonEmpty =>
          // Only last access time present
          lastAccess

        case _ =>
          // Both null or empty
          null
      }
    } catch {
      case _: Exception =>
        // If parsing fails, return whichever value is available, or null
        if (Option(completedOn).exists(_.nonEmpty)) completedOn
        else if (Option(lastContentAccessTime).exists(_.nonEmpty)) lastContentAccessTime
        else null
    }
  }

  def prepareReport(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame)(implicit fc: FrameworkContext, config: JobConfig): DataFrame = {
    implicit val sparkSession: SparkSession = spark
    val userEnrolmentDF = getUserEnrollment(spark, fetchData)
    val userCachedDF = getUserCacheDF(spark, fetchData)
    val courseBatchDF = getCourseBatchDF(spark, fetchData) // Now includes batch name

    // Clean user enrolments and course batch
    val cleanUserEnrolmentDF = userEnrolmentDF.filter(
      col("courseid").isNotNull &&
        col("batchid").isNotNull &&
        col("courseid") =!= "" &&
        col("batchid") =!= ""
    )

    val cleanCourseBatchDF = courseBatchDF.filter(
      col("courseid").isNotNull &&
        col("batchid").isNotNull &&
        col("courseid") =!= "" &&
        col("batchid") =!= ""
    )

    // Join enrolments with course batch (to get start & end dates and batch name)
    val userJoinedWithBatchDF = cleanUserEnrolmentDF.join(cleanCourseBatchDF, Seq("courseid", "batchid"), "left")

    val convertDate = spark.udf.register("convertDate", convertDateFn)
    val calculateTimeSpentUDF = spark.udf.register("calculateTimeSpent", calculateTimeSpent _)

    // Apply date transformations
    val userJoinedWithBatchConvertedDF = userJoinedWithBatchDF
      .withColumn("start_date", convertDate(col("start_date")))
      .withColumn("end_date", convertDate(col("end_date")))
      .withColumn("time_spent", calculateTimeSpentUDF(col("completedon"), col("lastcontentaccesstime")))

    val formatCompletionDateUDF = spark.udf.register("formatCompletionDate", formatCompletionDate _)
    val getReadableStatusUDF = spark.udf.register("getReadableStatus", getReadableStatus _)

    // Create a struct containing courseid, batchid, batch name, dates, and progression
    val userJoinedWithCourseStruct = userJoinedWithBatchConvertedDF
      .withColumn("course_batch_info", struct(
        col("courseid"),
        col("batchid"),
        col("name").as("batch_name"),
        col("start_date"),
        col("end_date"),
        coalesce(col("enrolled_date"), col("enrolleddate")).cast("string").as("enrolled_date"),
        formatCompletionDateUDF(col("completedon")).as("completedon"),
        getReadableStatusUDF(col("status")).as("current_status"),
        coalesce(col("completionpercentage"), lit(0)).cast("int").as("progression"),
        col("time_spent")
      ))

    // Compute user-level metrics (rest remains the same)
    val userCourseAggDF = userJoinedWithCourseStruct.groupBy("userid").agg(
      size(collect_set(when(col("enrolleddate").isNotNull, col("course_batch_info")))).as("num_courses_enrolled"),
      size(collect_set(when((col("status") === 1) && col("enrolleddate").isNotNull, col("course_batch_info")))).as("num_courses_started"),
      size(collect_set(when((col("status") === 2) && col("enrolleddate").isNotNull, col("course_batch_info")))).as("num_courses_completed"),

      collect_set(when(col("enrolleddate").isNotNull, col("course_batch_info"))).as("courses_enrolled"),
      collect_set(when((col("status") === 1) && col("enrolleddate").isNotNull, col("course_batch_info"))).as("courses_started"),
      collect_set(when((col("status") === 2) && col("enrolleddate").isNotNull, col("course_batch_info"))).as("courses_completed")
    )
    // Join back to user info for reporting
    val userSummaryDF = userCachedDF.join(userCourseAggDF, Seq("userid"), "left")
      .na.fill(0, Seq("num_courses_enrolled", "num_courses_started", "num_courses_completed"))
    val decryptedSummary = decryptUserInfo(userSummaryDF)

    // Step 1: Collect all unique course IDs from the DataFrame
    val allCourseIds = decryptedSummary
      .select(explode(flatten(array(col("courses_enrolled"), col("courses_started"), col("courses_completed")))))
      .filter(col("col").isNotNull)
      .select(col("col.courseid").as("courseid"))
      .distinct()
      .rdd.map(r => r.getString(0)).collect().toList.filter(_ != null)

    // Step 2: Fetch course and batch details
    val (courseDetailsMap, batchDetailsMap) = getCourseDetails(allCourseIds, courseBatchDF)
    val broadcastedCourseMap = spark.sparkContext.broadcast(courseDetailsMap)
    val broadcastedBatchMap = spark.sparkContext.broadcast(batchDetailsMap)

    // Step 3: Updated UDF to include batch name and progression
    val enrichCoursesUDF = udf((courseStructs: Seq[Row], statusFilter: String) => {
      if (courseStructs == null) null
      else {
        val courseMap = broadcastedCourseMap.value
        val batchMap = broadcastedBatchMap.value
        courseStructs.filter(_ != null).map { courseStruct =>
          val courseid = Option(courseStruct.getAs[String]("courseid")).getOrElse("")
          val batchid = Option(courseStruct.getAs[String]("batchid")).getOrElse("")
          val batchName = Option(courseStruct.getAs[String]("batch_name")).getOrElse("")
          val startDate = Option(courseStruct.getAs[String]("start_date")).getOrElse("")
          val endDate = Option(courseStruct.getAs[String]("end_date")).getOrElse("")
          val enrolledDate = Option(courseStruct.getAs[String]("enrolled_date")).getOrElse("")
          val completedOn = Option(courseStruct.getAs[String]("completedon")).getOrElse("")
          val currentStatus = Option(courseStruct.getAs[String]("current_status")).getOrElse("")
          val progression = Option(courseStruct.getAs[Int]("progression")).getOrElse(0)
          val timeSpent = Option(courseStruct.getAs[String]("time_spent")).getOrElse("")

          val (code, name) = courseMap.getOrElse(courseid, ("", ""))
          val finalBatchName = if (batchName.nonEmpty) batchName else batchMap.getOrElse((courseid, batchid), "")

          // Base map with common fields - ensure all values are String type
          val baseMap = Map[String, String](
            "course_id" -> courseid,
            "batch_id" -> batchid,
            "batch_name" -> finalBatchName,
            "name" -> name,
            "code" -> code,
            "start_date" -> startDate,
            "end_date" -> endDate
          )

          statusFilter match {
            case "enrolled" => baseMap ++ Map("enrolled_date" -> enrolledDate, "current_status" -> currentStatus)
            case "started" => baseMap ++ Map("progression" -> progression.toString) // Convert to String
            case "completed" => baseMap ++ Map(
              "course_completed_date" -> completedOn,
              "progression" -> progression.toString, // Convert to String
              "time_spent" -> timeSpent
            )
            case _ => baseMap
          }
        }
      }
    })

    val withEnriched = decryptedSummary
      .withColumn("courses_enrolled", enrichCoursesUDF(col("courses_enrolled"), lit("enrolled")))
      .withColumn("courses_started", enrichCoursesUDF(col("courses_started"), lit("started")))
      .withColumn("courses_completed", enrichCoursesUDF(col("courses_completed"), lit("completed")))

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

  def getCourseDetails(courseIds: List[String], courseBatchDF: DataFrame)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): (Map[String, (String, String)], Map[(String, String), String]) = {
    if (courseIds.isEmpty) return (Map.empty, Map.empty)

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

    // Course details map: courseId -> (code, name)
    val courseMap = courseDetails.map(c => c.identifier -> (c.code, c.name)).toMap

    // Batch details map: (courseId, batchId) -> batchName
    val batchMap: Map[(String, String), String] = courseBatchDF
      .select(col("courseid"), col("batchid"), col("name").as("batch_name"))
      .rdd.map(r => ((r.getString(0), r.getString(1)), r.getString(2)))
      .collect().toMap

    (courseMap, batchMap)
  }

  // Fixed method signature - returns (code, name) as a tuple. Returns ("", "") if not found.
  def getCourseCodeAndName(courseId: List[String])(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): (String, String) = {
    case class CollectionDetails(result: Map[String, List[Map[String, String]]])
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

    try {
      // Use the fixed case class without AnyRef
      val response = RestUtil.post[CollectionDetails](apiURL, request)
      val contentList = response.result.getOrElse("content", List.empty[Map[String, String]])

      if (contentList.nonEmpty) {
        val firstCourse = contentList.head
        val code = firstCourse.getOrElse("code", "")
        val name = firstCourse.getOrElse("name", "")
        (code, name)
      } else {
        ("", "")
      }
    } catch {
      case e: Exception =>
        JobLogger.log("Error fetching course code and name from API", Option(Map("error" -> e.getMessage)), INFO)
        ("", "")
    }
  }

  // Date formatting methods borrowed from CourseBatchStatusUpdaterJob
  def getDateFormat(): SimpleDateFormat = {
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
    dateFormatter.setTimeZone(TimeZone.getTimeZone("IST"))
    dateFormatter
  }

  def formatDate(date: String): String = {
    Option(date).map(x => {
      getDateFormat().format(getDateFormat().parse(x))
    }).orNull
  }

  def convertDateFn: String => String = (date: String) => {
    Option(date).map(x => {
      val utcDateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      utcDateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"))
      getDateFormat().format(utcDateFormatter.parse(x))
    }).orNull
  }

  // Create UDF for date formatting
  val formatDateUDF = udf((date: String) => formatDate(date))

  // UDF to format completion date from ISO format to readable format
  def formatCompletionDate(date: String): String = {
    Option(date).map(x => {
      try {
        val isoFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        isoFormatter.setTimeZone(TimeZone.getTimeZone("UTC"))
        val parsedDate = isoFormatter.parse(x)

        val istFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        istFormatter.setTimeZone(TimeZone.getTimeZone("IST"))
        istFormatter.format(parsedDate)
      } catch {
        case _: Exception => x // Return original if parsing fails
      }
    }).orNull
  }

  val formatCompletionDateUDF = udf((date: String) => formatCompletionDate(date))

  // UDF to convert numeric status to readable status
  def getReadableStatus(status: Int): String = {
    status match {
      case 0 => "Not Started"
      case 1 => "In Progress"
      case 2 => "Completed"
      case _ => "Null"
    }
  }

  val getReadableStatusUDF = udf((status: Int) => getReadableStatus(status))
}