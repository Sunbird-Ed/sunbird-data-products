package org.sunbird.analytics.job.report

import java.util.concurrent.atomic.AtomicInteger

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.util.Constants
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.sunbird.analytics.util.{CourseResponse, CourseUtils, UserCache, UserData}
import org.sunbird.cloud.storage.conf.AppConf

case class CourseInfo(courseid: String, batchid: String, startdate: String, enddate: String, channel: String)

case class CourseBatchOutput(courseid: String, batchid: String, startdate: String, enddate: String)

object AssessmentMetricsJobV2 extends optional.Application with IJob with BaseReportsJob {

  implicit val className = "org.ekstep.analytics.job.AssessmentMetricsJobV2"

  val metrics = scala.collection.mutable.Map[String, BigInt]();
  val sunbirdKeyspace = AppConf.getConfig("course.metrics.cassandra.sunbirdKeyspace")
  val sunbirdCoursesKeyspace = AppConf.getConfig("course.metrics.cassandra.sunbirdCoursesKeyspace")
  val cassandraUrl = "org.apache.spark.sql.cassandra"

  // $COVERAGE-OFF$ Disabling scoverage for main and execute method
  def name(): String = "AssessmentMetricsJobV2"

  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
    JobLogger.init("Assessment Metrics")
    JobLogger.start("Assessment Job Started executing", Option(Map("config" -> config, "model" -> name)))

    val conf = config.split(";")
    val batchIds = if (conf.length > 1) {
      conf(1).split(",").toList
    } else List()

    val jobConfig = JSONUtils.deserialize[JobConfig](conf(0))
    JobContext.parallelization = CommonUtil.getParallelization(jobConfig);
    implicit val sparkContext: SparkContext = getReportingSparkContext(jobConfig);
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext();
    execute(jobConfig, batchIds)
  }

  private def execute(config: JobConfig, batchList: List[String])(implicit sc: SparkContext, fc: FrameworkContext) = {
    val readConsistencyLevel: String = AppConf.getConfig("assessment.metrics.cassandra.input.consistency")
    val sparkConf = sc.getConf
      .set("spark.cassandra.input.consistency.level", readConsistencyLevel)
      .set("spark.sql.caseSensitive", AppConf.getConfig(key = "spark.sql.caseSensitive"))
    val spark: SparkSession = SparkSession.builder.config(sparkConf).getOrCreate()
    val time = CommonUtil.time({
      prepareReport(spark, loadData, config, batchList)
    });
    metrics.put("totalExecutionTime", time._1);
    JobLogger.end("AssessmentReport Generation Job completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name, "metrics" -> metrics)))
    spark.stop()
    fc.closeContext()
  }

  // $COVERAGE-ON$ Enabling scoverage for all other functions
  def recordTime[R](block: => R, msg: String): (R) = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    JobLogger.log(msg + (t1 - t0), None, INFO)
    result;
  }

  /**
   * Generic method used to load data by passing configurations
   *
   * @param spark    - Spark Sessions
   * @param settings - Cassandra/Redis configs
   * @param url      - Cassandra/Redis url
   * @return
   */
  def loadData(spark: SparkSession, settings: Map[String, String], url: String, schema: StructType): DataFrame = {
    if (schema.nonEmpty) {
      spark.read.schema(schema).format(url).options(settings).load()
    }
    else {
      spark.read.format(url).options(settings).load()
    }
  }

  def prepareReport(spark: SparkSession, loadData: (SparkSession, Map[String, String], String, StructType) => DataFrame, config: JobConfig, batchList: List[String])(implicit fc: FrameworkContext): Unit = {

    implicit val sparkSession: SparkSession = spark
    implicit val sqlContext = new SQLContext(spark.sparkContext)
    import sqlContext.implicits._
    val batches = CourseUtils.getActiveBatches(loadData, batchList, sunbirdCoursesKeyspace)
    val userData = CommonUtil.time({
      recordTime(getUserData(spark, loadData), "Time taken to get generate the userData- ")
    })
    val modelParams = config.modelParams.get
    val contentFilters = modelParams.getOrElse("contentFilters",Map()).asInstanceOf[Map[String,AnyRef]]
    val reportPath = modelParams.getOrElse("reportPath", AppConf.getConfig("assessment.metrics.cloud.objectKey")).asInstanceOf[String]

    val filteredBatches = if(contentFilters.nonEmpty) {
      val filteredContents = CourseUtils.filterContents(spark, JSONUtils.serialize(contentFilters)).toDF()
      batches.join(filteredContents, batches.col("courseid") === filteredContents.col("identifier"), "inner")
        .select(batches.col("*")).collect
    } else batches.collect

    val activeBatchesCount = new AtomicInteger(filteredBatches.length)
    metrics.put("userDFLoadTime", userData._1)
    metrics.put("activeBatchesCount", activeBatchesCount.get())
    val batchFilters = JSONUtils.serialize(modelParams("batchFilters"))
    val uploadToAzure = AppConf.getConfig("course.upload.reports.enabled")
    val tempDir = AppConf.getConfig("assessment.metrics.temp.dir")
    val assessmentProfileDF = getAssessmentProfileDF(loadData).persist(StorageLevel.MEMORY_ONLY)
    for (index <- filteredBatches.indices) {
      val row = filteredBatches(index)
      val courses = CourseUtils.getCourseInfo(spark, row.getString(0))
      val batch = CourseBatch(row.getString(1), row.getString(2), row.getString(3), courses.channel);
      if (null != courses.framework && courses.framework.nonEmpty && batchFilters.toLowerCase.contains(courses.framework.toLowerCase)) {
        val result = CommonUtil.time({
          val reportDF = recordTime(getReportDF(batch, userData._2, assessmentProfileDF), s"Time taken to generate DF for batch ${batch.batchid} - ")
          val contentIds: List[String] = reportDF.select(col("content_id")).distinct().collect().map(_ (0)).toList.asInstanceOf[List[String]]
          if (contentIds.nonEmpty) {
            val denormalizedDF = recordTime(denormAssessment(reportDF, contentIds.distinct).persist(StorageLevel.MEMORY_ONLY), s"Time take to denorm the assessment - ")
            val totalRecords = denormalizedDF.count()
            if (totalRecords > 0) recordTime(saveReport(denormalizedDF, tempDir, uploadToAzure, batch.batchid, reportPath), s"Time take to save the $totalRecords for batch ${batch.batchid} all the reports into azure -")
            denormalizedDF.unpersist(true)
          }
          reportDF.unpersist(true)
        })
        JobLogger.log(s"Time taken to generate report for batch ${batch.batchid} is ${result._1}. Remaining batches - ${activeBatchesCount.getAndDecrement()}", None, INFO)
      } else {
        JobLogger.log(s"Constrains are not matching, skipping the courseId: ${row.getString(0)}, batchId: ${batch.batchid} and Remaining batches - ${activeBatchesCount.getAndDecrement()}", None, INFO)
      }
    }
    assessmentProfileDF.unpersist(true)
    userData._2.unpersist(true)
  }

  def getUserData(spark: SparkSession, loadData: (SparkSession, Map[String, String], String, StructType) => DataFrame): DataFrame = {
    val schema = Encoders.product[UserData].schema
    loadData(spark, Map("table" -> "user", "infer.schema" -> "true", "key.column" -> "userid"), "org.apache.spark.sql.redis", schema)
      .withColumn("username", concat_ws(" ", col("firstname"), col("lastname"))).persist(StorageLevel.MEMORY_ONLY)
  }

  def getUserEnrollmentDF(loadData: (SparkSession, Map[String, String], String, StructType) => DataFrame)(implicit spark: SparkSession): DataFrame = {
    loadData(spark, Map("table" -> "user_enrolments", "keyspace" -> sunbirdCoursesKeyspace), cassandraUrl, new StructType())
      .filter(lower(col("active")).equalTo("true"))
      .select(col("batchid"), col("userid"), col("courseid"), col("active")
        , col("completionpercentage"), col("enrolleddate"), col("completedon"))
  }

  def getAssessmentProfileDF(loadData: (SparkSession, Map[String, String], String, StructType) => DataFrame)(implicit spark: SparkSession): DataFrame = {
    val userEnrolmentDF = getUserEnrollmentDF(loadData).persist(StorageLevel.MEMORY_ONLY)
    val assessmentProfileDF = loadData(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace), cassandraUrl, new StructType())
      .select("course_id", "batch_id", "user_id", "content_id", "total_max_score", "total_score", "grand_total").persist(StorageLevel.MEMORY_ONLY)
    val assessmentDF = getAssessmentData(assessmentProfileDF)

    /**
     * Compute the sum of all the worksheet contents score.
     */
    val assessmentAggDf = Window.partitionBy("user_id", "batch_id", "course_id")
    val aggregatedAssessmentDF = assessmentDF
      .withColumn("agg_score", sum("total_score") over assessmentAggDf)
      .withColumn("agg_max_score", sum("total_max_score") over assessmentAggDf)
      .withColumn("total_sum_score", concat(ceil((col("agg_score") * 100) / col("agg_max_score")), lit("%")))

    val reportDF = userEnrolmentDF.join(aggregatedAssessmentDF,
      userEnrolmentDF.col("userid") === aggregatedAssessmentDF.col("user_id")
        && userEnrolmentDF.col("batchid") === aggregatedAssessmentDF.col("batch_id")
        && userEnrolmentDF.col("courseid") === aggregatedAssessmentDF.col("course_id"), "inner")
      .select(userEnrolmentDF.col("batchid"),
        userEnrolmentDF.col("active"),
        userEnrolmentDF.col("courseid"),
        userEnrolmentDF.col("userid"),
        aggregatedAssessmentDF.col("content_id"),
        aggregatedAssessmentDF.col("total_score"),
        aggregatedAssessmentDF.col("grand_total"), aggregatedAssessmentDF.col("total_sum_score"))
      .persist(StorageLevel.MEMORY_ONLY)

    userEnrolmentDF.unpersist(true)
    reportDF
  }

  def getReportDF(batch: CourseBatch, userDF: DataFrame, assessmentProfileDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    JobLogger.log("Creating report for batch " + batch.batchid, None, INFO)
    val filteredAssessmentProfileDF = assessmentProfileDF.where(col("batchid") === batch.batchid)
      .withColumn("enddate", lit(batch.endDate))
      .withColumn("startdate", lit(batch.startDate))
      .withColumn("channel", lit(batch.courseChannel))
      .select(col("batchid"),
        col("enddate"),
        col("startdate"),
        col("channel"),
        col("userid"),
        col("courseid"),
        col("active"),
        col("content_id"),
        col("total_score"),
        col("grand_total"),
        col("total_sum_score")
      )
    filteredAssessmentProfileDF
      .join(userDF, Seq("userid"), "inner")
      .withColumn(UserCache.externalid, when(filteredAssessmentProfileDF.col("channel") === userDF.col(UserCache.userchannel), userDF.col(UserCache.externalid)).otherwise(""))
      .withColumn(UserCache.schoolname, when(filteredAssessmentProfileDF.col("channel") === userDF.col(UserCache.userchannel), userDF.col(UserCache.schoolname)).otherwise(""))
      .withColumn(UserCache.block, when(filteredAssessmentProfileDF.col("channel") === userDF.col(UserCache.userchannel), userDF.col(UserCache.block)).otherwise(""))
      .withColumn(UserCache.schooludisecode, when(filteredAssessmentProfileDF.col("channel") === userDF.col(UserCache.userchannel), userDF.col(UserCache.schooludisecode)).otherwise(""))
      .select(filteredAssessmentProfileDF.col("*"),
        col(UserCache.firstname),
        col(UserCache.lastname),
        col(UserCache.maskedemail),
        col(UserCache.maskedphone),
        col(UserCache.district),
        col(UserCache.externalid),
        col(UserCache.schoolname),
        col(UserCache.schooludisecode),
        col(UserCache.state),
        col(UserCache.orgname),
        col("username")).persist(StorageLevel.MEMORY_ONLY)
      .filter(col("content_id").isNotNull)
  }


  /**
   * Get the Either last updated assessment question or Best attempt assessment
   *
   * @param reportDF - Dataframe, Report df.
   * @return DataFrame
   */
  def getAssessmentData(reportDF: DataFrame): DataFrame = {
    val bestScoreReport = AppConf.getConfig("assessment.metrics.bestscore.report").toBoolean
    val columnName: String = if (bestScoreReport) "total_score" else "last_attempted_on"
    val df = Window.partitionBy("user_id", "batch_id", "course_id", "content_id").orderBy(desc(columnName))
    reportDF.withColumn("rownum", row_number.over(df)).where(col("rownum") === 1).drop("rownum").persist(StorageLevel.MEMORY_ONLY)
  }

  /**
   * De-norming the assessment report - Adding content name column to the content id
   *
   * @return - Assessment denormalised dataframe
   */
  def denormAssessment(report: DataFrame, contentIds: List[String])(implicit spark: SparkSession): DataFrame = {
    val contentMetaDataDF = getAssessmentNames(spark, contentIds, AppConf.getConfig("assessment.metrics.supported.contenttype"))
    report.join(contentMetaDataDF, report.col("content_id") === contentMetaDataDF.col("identifier"), "right_outer") // Doing right join since to generate report only for the "SelfAssess" content types
      .select(
        col("name").as("content_name"),
        col("total_sum_score"), report.col(UserCache.userid), report.col("courseid"), report.col("batchid"),
        col("grand_total"), report.col(UserCache.maskedemail), report.col(UserCache.district), report.col(UserCache.maskedphone),
        report.col(UserCache.orgname), report.col(UserCache.externalid), report.col(UserCache.schoolname),
        report.col("username"), col(UserCache.state), col(UserCache.schooludisecode))
  }

  def getAssessmentNames(spark: SparkSession, content: List[String], contentType: String): DataFrame = {
    implicit val sqlContext = new SQLContext(spark.sparkContext)
    import sqlContext.implicits._

    val apiUrl = Constants.COMPOSITE_SEARCH_URL
    val contentList = JSONUtils.serialize(content)

    val request =
      s"""
         |{
         |  "request": {
         |    "filters": {
         |      "contentType": "$contentType",
         |       "identifier": $contentList
         |    },
         |    "sort_by": {"createdOn":"desc"},
         |    "limit": 10000,
         |    "fields": ["name","identifer","contentType"]
         |  }
         |}
       """.stripMargin
    val response = RestUtil.post[CourseResponse](apiUrl, request)
    val assessmentInfo = if (null != response && response.responseCode.equalsIgnoreCase("ok") && response.result.count > 0) {
      response.result.content
    } else List()

    assessmentInfo.toDF().select("name", "identifier")
  }

  /**
   * This method is used to upload the report the azure cloud service
   */
  def saveReport(reportDF: DataFrame, url: String, uploadToAzure: String, batchid: String, reportPath: String)(implicit spark: SparkSession, fc: FrameworkContext): Unit = {
    if (StringUtils.isNotBlank(uploadToAzure) && StringUtils.equalsIgnoreCase("true", uploadToAzure)) {
      save(reportDF, url, spark, batchid, reportPath)
    } else {
      JobLogger.log("Skipping uploading reports into to azure", None, INFO)
    }
  }

  /**
   * Converting rows into  column (Reshaping the dataframe.)
   * This method converts the name column into header row formate
   * Example:
   * Input DF
   * +------------------+-------+--------------------+-------+-----------+
   * |              name| userid|            courseid|batchid|total_score|
   * +------------------+-------+--------------------+-------+-----------+
   * |Playingwithnumbers|user021|do_21231014887798...|   1001|         10|
   * |     Whole Numbers|user021|do_21231014887798...|   1001|          4|
   * +------------------+---------------+-------+--------------------+----
   *
   * Output DF: After re-shaping the data frame.
   * +--------------------+-------+-------+------------------+-------------+
   * |            courseid|batchid| userid|Playingwithnumbers|Whole Numbers|
   * +--------------------+-------+-------+------------------+-------------+
   * |do_21231014887798...|   1001|user021|                10|            4|
   * +--------------------+-------+-------+------------------+-------------+
   * Example:
   */
  def transposeDF(reportDF: DataFrame): DataFrame = {
    // Re-shape the dataFrame (Convert the content name from the row to column)
    reportDF.groupBy("courseid", "batchid", "userid")
      .pivot("content_name").agg(concat(ceil((split(first("grand_total"), "\\/")
      .getItem(0) * 100) / (split(first("grand_total"), "\\/")
      .getItem(1))), lit("%")))
  }

  def saveToAzure(reportDF: DataFrame, url: String, batchId: String, transposedData: DataFrame, reportPath: String): String = {
    val storageConfig = getStorageConfig(AppConf.getConfig("cloud.container.reports"), reportPath)
    val azureData = reportDF.select(
      reportDF.col(UserCache.externalid).as("External ID"),
      reportDF.col(UserCache.userid).as("User ID"),
      reportDF.col("username").as("User Name"),
      reportDF.col(UserCache.maskedemail).as("Email ID"),
      reportDF.col(UserCache.maskedphone).as("Mobile Number"),
      reportDF.col(UserCache.orgname).as("Organisation Name"),
      reportDF.col(UserCache.state).as("State Name"),
      reportDF.col(UserCache.district).as("District Name"),
      reportDF.col(UserCache.schooludisecode).as("School UDISE Code"),
      reportDF.col(UserCache.schoolname).as("School Name"),
      transposedData.col("*"), // Since we don't know the content name column so we are using col("*")
      reportDF.col("total_sum_score").as("Total Score"))
      .drop(UserCache.userid, "courseid", "batchid")
    azureData.saveToBlobStore(storageConfig, "csv", "report-" + batchId, Option(Map("header" -> "true")), None);
    s"${AppConf.getConfig("cloud.container.reports")}/${AppConf.getConfig("assessment.metrics.cloud.objectKey")}/report-$batchId.csv"

  }

  def save(reportDF: DataFrame, url: String, spark: SparkSession, batchId: String, reportPath: String)(implicit fc: FrameworkContext): Unit = {
    val transposedData = transposeDF(reportDF)
    val reportData = transposedData.join(reportDF, Seq("courseid", "batchid", "userid"), "inner")
      .dropDuplicates("userid", "courseid", "batchid").drop("content_name")
    try {
      recordTime(saveToAzure(reportData, url, batchId, transposedData, reportPath), s"Time taken to save the $batchId into azure -")
    } catch {
      case e: Exception => JobLogger.log("File upload is failed due to " + e, None, ERROR)
    }
  }

}