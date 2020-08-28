package org.sunbird.analytics.job.report

import java.util.concurrent.atomic.AtomicInteger

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, unix_timestamp, _}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework.Level._
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.sunbird.analytics.util.{CourseUtils, UserCache, UserData}
import org.sunbird.cloud.storage.conf.AppConf

import scala.collection.mutable

trait ReportGeneratorV2 {

  def loadData(spark: SparkSession, settings: Map[String, String], url: String, schema: StructType): DataFrame

  def prepareReport(spark: SparkSession, storageConfig: StorageConfig, fetchTable: (SparkSession, Map[String, String], String, StructType) => DataFrame, config: JobConfig, batchList: List[String])(implicit fc: FrameworkContext): Unit
}

case class CourseData(courseid: String, leafNodesCount: String, level1Data: List[Level1Data])
case class Level1Data(l1identifier: String, l1leafNodesCount: String)
case class UserAggData(user_id: String, activity_id: String, completedCount: Int, context_id: String)

object CourseMetricsJobV2 extends optional.Application with IJob with ReportGeneratorV2 with BaseReportsJob {

  implicit val className: String = "org.ekstep.analytics.job.CourseMetricsJobV2"
  val sunbirdKeyspace: String = AppConf.getConfig("course.metrics.cassandra.sunbirdKeyspace")
  val sunbirdCoursesKeyspace: String = AppConf.getConfig("course.metrics.cassandra.sunbirdCoursesKeyspace")
  val sunbirdHierarchyStore: String = AppConf.getConfig("course.metrics.cassandra.sunbirdHierarchyStore")
  val metrics: mutable.Map[String, BigInt] = mutable.Map[String, BigInt]()

  // $COVERAGE-OFF$ Disabling scoverage for main and execute method
  def name(): String = "CourseMetricsJobV2"

  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
    JobLogger.init("CourseMetricsJob")
    JobLogger.start("CourseMetrics Job Started executing", Option(Map("config" -> config, "model" -> name)))

    val conf = config.split(";")
    val batchIds = if (conf.length > 1) {
      conf(1).split(",").toList
    } else List()
    val jobConfig = JSONUtils.deserialize[JobConfig](conf(0))
    JobContext.parallelization = CommonUtil.getParallelization(jobConfig)

    implicit val sparkContext: SparkContext = getReportingSparkContext(jobConfig)
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()
    execute(jobConfig, batchIds)
  }

  private def execute(config: JobConfig, batchList: List[String])(implicit sc: SparkContext, fc: FrameworkContext) = {
    val readConsistencyLevel: String = AppConf.getConfig("course.metrics.cassandra.input.consistency")
    val sparkConf = sc.getConf
      .set("es.write.operation", "upsert")
      .set("spark.cassandra.input.consistency.level", readConsistencyLevel)

    val container = AppConf.getConfig("cloud.container.reports")
    val objectKey = AppConf.getConfig("course.metrics.cloud.objectKey")
    val storageConfig = getStorageConfig(container, objectKey)
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val time = CommonUtil.time({
      prepareReport(spark, storageConfig, loadData, config, batchList)
    })
    metrics.put("totalExecutionTime", time._1)
    JobLogger.end("CourseMetrics Job completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name, "metrics" -> metrics)))
    fc.closeContext()
  }

  // $COVERAGE-ON$ Enabling scoverage for all other functions
  def loadData(spark: SparkSession, settings: Map[String, String], url: String, schema: StructType): DataFrame = {
    if (schema.nonEmpty) {
      spark.read.schema(schema).format(url).options(settings).load()
    }
    else {
      spark.read.format(url).options(settings).load()
    }
  }

  def getUserCourseInfo(loadData: (SparkSession, Map[String, String], String, StructType) => DataFrame)(implicit spark: SparkSession): DataFrame = {
    implicit val sqlContext: SQLContext = spark.sqlContext
    import sqlContext.implicits._

    val userAgg = loadData(spark, Map("table" -> "user_activity_agg", "keyspace" -> sunbirdCoursesKeyspace), "org.apache.spark.sql.cassandra", new StructType())
      .select("user_id","activity_id","agg","context_id").map(row => {
      UserAggData(row.getString(0),row.getString(1),row.get(2).asInstanceOf[Map[String,Int]]("completedCount"),row.getString(3))
    }).toDF()

    val hierarchyData = loadData(spark, Map("table" -> "content_hierarchy", "keyspace" -> sunbirdHierarchyStore), "org.apache.spark.sql.cassandra", new StructType())
      .select("identifier","hierarchy")

    val hierarchyDataDf = hierarchyData.rdd.map(row => {
      val hierarchy = JSONUtils.deserialize[Map[String,AnyRef]](row.getString(1))
      parseCourseHierarchy(List(hierarchy),0, CourseData(row.getString(0),"0",List()))
    }).toDF()

    val hierarchyDf = hierarchyDataDf.select($"courseid",$"leafNodesCount",$"level1Data", explode_outer($"level1Data").as("exploded_level1Data"))
      .select("courseid","leafNodesCount","exploded_level1Data.*")

    val dataDf = hierarchyDf.join(userAgg,hierarchyDf.col("courseid") === userAgg.col("activity_id"), "left")
      .withColumn("completionPercentage", (userAgg.col("completedCount")/hierarchyDf.col("leafNodesCount")*100).cast("int"))
      .select(userAgg.col("user_id").as("userid"),
        userAgg.col("context_id").as("contextid"),
        hierarchyDf.col("courseid"),
        col("completionPercentage"),
        hierarchyDf.col("l1identifier"),
        hierarchyDf.col("l1leafNodesCount"))

    val resDf = dataDf.join(userAgg, dataDf.col("l1identifier") === userAgg.col("activity_id") &&
      userAgg.col("context_id") === dataDf.col("contextid") && userAgg.col("user_id") === dataDf.col("userid"),"left")
      .withColumn("l1completionPercentage", (userAgg.col("completedCount")/dataDf.col("l1leafNodesCount")*100).cast("int"))
      .select(col("userid"),
        col("courseid"),
        col("contextid"),
        col("completionPercentage"),
        col("l1identifier"),
        col("l1completionPercentage"))
    resDf
  }

  def parseCourseHierarchy(data: List[Map[String,AnyRef]], levelCount: Int, prevData: CourseData): CourseData = {
    if(levelCount < 2) {
      val list = data.map(childNodes => {
        val mimeType = childNodes.getOrElse("mimeType","").asInstanceOf[String]
        val visibility = childNodes.getOrElse("visibility","").asInstanceOf[String]
        val contentType = childNodes.getOrElse("contentType","").asInstanceOf[String]
        if((StringUtils.equalsIgnoreCase(mimeType,"application/vnd.ekstep.content-collection") && StringUtils.equalsIgnoreCase(visibility,"Default") && StringUtils.equalsIgnoreCase(contentType,"Course"))) {
          val identifier = childNodes.getOrElse("identifier","").asInstanceOf[String]
          val leafNodesCount = childNodes.getOrElse("leafNodesCount",0).asInstanceOf[Int]
          val courseData = if (levelCount == 0) {
            CourseData(prevData.courseid, leafNodesCount.toString, List())
          } else {
            val prevL1List = prevData.level1Data
            CourseData(prevData.courseid, prevData.leafNodesCount, (prevL1List ::: List(Level1Data(identifier, leafNodesCount.toString))))
          }
          val children = childNodes.getOrElse("children",List()).asInstanceOf[List[Map[String,AnyRef]]]
          if(null != children && children.nonEmpty) {
            parseCourseHierarchy(children, levelCount+1, courseData)
          } else courseData
        } else prevData
      })
      val courseId = list.head.courseid
      val leafNodeCount = list.head.leafNodesCount
      val level1Data = list.map(x => x.level1Data).flatten.toList
      CourseData(courseId, leafNodeCount, level1Data)
    } else prevData
  }

  def recordTime[R](block: => R, msg: String): R = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    JobLogger.log(msg + (t1 - t0), None, INFO)
    result
  }

  def prepareReport(spark: SparkSession, storageConfig: StorageConfig, loadData: (SparkSession, Map[String, String], String, StructType) => DataFrame, config: JobConfig, batchList: List[String])(implicit fc: FrameworkContext): Unit = {

    implicit val sparkSession: SparkSession = spark
    implicit val sqlContext: SQLContext = spark.sqlContext
    import sqlContext.implicits._
    val activeBatches = CourseUtils.getActiveBatches(loadData, batchList, sunbirdCoursesKeyspace)
    val modelParams = config.modelParams.get
    val contentFilters = modelParams.getOrElse("contentFilters", Map()).asInstanceOf[Map[String,AnyRef]]
    val reportPath = modelParams.getOrElse("reportPath","course-progress-reports/").asInstanceOf[String]

    val filteredBatches = if(contentFilters.nonEmpty) {
      val filteredContents = CourseUtils.filterContents(spark, JSONUtils.serialize(contentFilters)).toDF()
      activeBatches.join(filteredContents, activeBatches.col("courseid") === filteredContents.col("identifier"), "inner")
        .select(activeBatches.col("*")).collect
    } else activeBatches.collect

    val userCourses = getUserCourseInfo(loadData).persist(StorageLevel.MEMORY_ONLY)
    val userData = CommonUtil.time({
      recordTime(getUserData(spark, loadData), "Time taken to get generate the userData- ")
    })
    val activeBatchesCount = new AtomicInteger(filteredBatches.length)
    metrics.put("userDFLoadTime", userData._1)
    metrics.put("activeBatchesCount", activeBatchesCount.get())
    val batchFilters = JSONUtils.serialize(modelParams("batchFilters"))
    val userEnrolmentDF = getUserEnrollmentDF(loadData).persist(StorageLevel.MEMORY_ONLY)

    val userCourseData = userCourses.join(userData._2, userCourses.col("userid") === userData._2.col("userid"), "inner")
      .select(userData._2.col("*"),
        userCourses.col("courseid"),
        userCourses.col("contextid"),
        userCourses.col("completionPercentage").as("course_completion"),
        userCourses.col("l1identifier"),
        userCourses.col("l1completionPercentage"))
    userCourseData.persist(StorageLevel.MEMORY_ONLY)

    for (index <- filteredBatches.indices) {
      val row = filteredBatches(index)
      val courses = CourseUtils.getCourseInfo(spark, row.getString(0))
      val batch = CourseBatch(row.getString(1), row.getString(2), row.getString(3), courses.channel);
      if (null != courses.framework && courses.framework.nonEmpty && batchFilters.toLowerCase.contains(courses.framework.toLowerCase)) {
        val result = CommonUtil.time({
          val reportDF = recordTime(getReportDF(batch, userCourseData, userEnrolmentDF), s"Time taken to generate DF for batch ${batch.batchid} - ")
          val totalRecords = reportDF.count()
          recordTime(saveReportToBlobStore(batch, reportDF, storageConfig, totalRecords, reportPath), s"Time taken to save report in blobstore for batch ${batch.batchid} - ")
          reportDF.unpersist(true)
        })
        JobLogger.log(s"Time taken to generate report for batch ${batch.batchid} is ${result._1}. Remaining batches - ${activeBatchesCount.getAndDecrement()}", None, INFO)
      } else {
        JobLogger.log(s"Constrains are not matching, skipping the courseId: ${row.getString(0)}, batchId: ${batch.batchid} and Remaining batches - ${activeBatchesCount.getAndDecrement()}", None, INFO)
      }
    }
    userData._2.unpersist(true)
    userEnrolmentDF.unpersist(true)
    userCourses.unpersist(true)
    userCourseData.unpersist(true)
  }

  def getUserData(spark: SparkSession, loadData: (SparkSession, Map[String, String], String, StructType) => DataFrame): DataFrame = {
    val schema = Encoders.product[UserData].schema
    loadData(spark, Map("table" -> "user", "infer.schema" -> "true", "key.column" -> "userid"), "org.apache.spark.sql.redis", schema)
      .withColumn("username", concat_ws(" ", col("firstname"), col("lastname"))).persist(StorageLevel.MEMORY_ONLY)
  }

  def getUserEnrollmentDF(loadData: (SparkSession, Map[String, String], String, StructType) => DataFrame)(implicit spark: SparkSession): DataFrame = {
    loadData(spark, Map("table" -> "user_enrolments", "keyspace" -> sunbirdCoursesKeyspace), "org.apache.spark.sql.cassandra", new StructType())
      .select(col("batchid"), col("userid"), col("courseid"), col("active"), col("certificates")
        , col("enrolleddate"), col("completedon"))
  }

  def getReportDF(batch: CourseBatch, userDF: DataFrame, userCourseDenormDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    JobLogger.log("Creating report for batch " + batch.batchid, None, INFO)
    /*
     * courseBatchDF has details about the course and batch details for which we have to prepare the report
     * courseBatchDF is the primary source for the report
     * userCourseDF has details about the user details enrolled for a particular course/batch
     */
    val userEnrolmentDF = userCourseDenormDF.where(col("batchid") === batch.batchid && lower(col("active")).equalTo("true"))
      .withColumn("enddate", lit(batch.endDate))
      .withColumn("startdate", lit(batch.startDate))
      .withColumn("channel", lit(batch.courseChannel))
      .withColumn("generatedOn", date_format(from_utc_timestamp(current_timestamp.cast(DataTypes.TimestampType), "Asia/Kolkata"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
      .withColumn("certificate_status", when(col("certificates").isNotNull && size(col("certificates").cast("array<map<string, string>>")) > 0, "Issued").otherwise(""))
      .select(
        col("batchid"),
        col("userid"),
        col("enrolleddate"),
        col("completedon"),
        col("active"),
        col("courseid"),
        col("generatedOn"),
        col("certificate_status"),
        col("channel")
      )
    val contextId = s"cb:${batch.batchid}"
    // userCourseDenormDF lacks some of the user information that need to be part of the report here, it will add some more user details
    val reportDF = userEnrolmentDF
      .join(userDF, userDF.col("contextid") === contextId &&
        userEnrolmentDF.col("courseid") === userDF.col("courseid") &&
        userEnrolmentDF.col("userid") === userDF.col("userid"), "inner")
      .select(
        userEnrolmentDF.col("*"),
        col("channel"),
        col(UserCache.firstname),
        col(UserCache.lastname),
        col(UserCache.maskedemail),
        col(UserCache.maskedphone),
        col(UserCache.externalid),
        col(UserCache.orgname),
        col(UserCache.schoolname),
        col(UserCache.district),
        col(UserCache.schooludisecode),
        col(UserCache.block),
        col(UserCache.state),
        col("course_completion"),
        col("l1identifier"),
        col("l1completionPercentage")
      ).persist(StorageLevel.MEMORY_ONLY)
    reportDF
  }

  def saveReportToBlobStore(batch: CourseBatch, reportDF: DataFrame, storageConfig: StorageConfig, totalRecords: Long, reportPath: String): Unit = {
    val transposeDF = reportDF.groupBy("courseid","batchid","userid")
      .pivot(concat(col("l1identifier"), lit(" - Progress"))).agg(concat(first("l1completionPercentage").cast("string"), lit("%")))
    val reportData = transposeDF.join(reportDF, Seq("courseid", "batchid", "userid"), "inner")
      .drop("l1identifier","l1completionPercentage")

    reportData
      .select(
        col(UserCache.externalid).as("External ID"),
        col(UserCache.userid).as("User ID"),
        concat_ws(" ", col(UserCache.firstname), col(UserCache.lastname)).as("User Name"),
        col(UserCache.maskedemail).as("Email ID"),
        col(UserCache.maskedphone).as("Mobile Number"),
        col(UserCache.orgname).as("Organisation Name"),
        col(UserCache.state).as("State Name"),
        col(UserCache.district).as("District Name"),
        col(UserCache.schooludisecode).as("School UDISE Code"),
        col(UserCache.schoolname).as("School Name"),
        col(UserCache.block).as("Block Name"),
        col("enrolleddate").as("Enrolment Date"),
        col("courseid").as("Course ID"),
        concat(col("course_completion").cast("string"), lit("%"))
          .as("Course Progress"),
        transposeDF.col("*"),
        col("completedon").as("Completion Date"),
        col("certificate_status").as("Certificate Status"))
      .drop("userid", "courseid", "batchid","null").distinct()
      .saveToBlobStore(storageConfig, "csv", reportPath + "report-" + batch.batchid, Option(Map("header" -> "true")), None)
    JobLogger.log(s"CourseMetricsJob: records stats before cloud upload: { batchId: ${batch.batchid}, totalNoOfRecords: $totalRecords }} ", None, INFO)
  }

}
