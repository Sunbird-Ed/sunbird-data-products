package org.sunbird.analytics.job.report

import java.util.concurrent.atomic.AtomicInteger

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.sunbird.analytics.util.{CourseUtils, UserCache}
import org.sunbird.cloud.storage.conf.AppConf

case class CourseInfo(courseid: String, batchid: String, startdate: String, enddate: String, channel: String)

case class CourseBatchOutput(courseid: String, batchid: String, startdate: String, enddate: String)

object AssessmentMetricsJobV2 extends optional.Application with IJob with BaseReportsJob {

  implicit val className = "org.ekstep.analytics.job.AssessmentMetricsJobV2"

  val finalColumnMapping = Map(UserCache.externalid -> "External ID", UserCache.userid -> "User ID",
    "username" -> "User Name", UserCache.maskedemail -> "Email ID", UserCache.maskedphone -> "Mobile Number",
    UserCache.orgname -> "Organisation Name", UserCache.state -> "State Name", UserCache.district -> "District Name",
    UserCache.schooludisecode -> "School UDISE Code", UserCache.schoolname -> "School Name", "total_sum_score" -> "Total Score")

  val finalColumnOrder = List("External ID", "User ID", "User Name", "Email ID", "Mobile Number", "Organisation Name",
    "State Name", "District Name", "School UDISE Code", "School Name", "Total Score")

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
      prepareReport(spark, fetchData, config, batchList)
    });
    metrics.put("totalExecutionTime", time._1);
    JobLogger.end("AssessmentReport Generation Job completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name, "metrics" -> metrics)))
    spark.stop()
    fc.closeContext()
  }

  def prepareReport(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, Option[StructType], Option[Seq[String]]) => DataFrame, config: JobConfig, batchList: List[String])(implicit fc: FrameworkContext): Unit = {

    implicit val sparkSession: SparkSession = spark
    implicit val sqlContext = new SQLContext(spark.sparkContext)
    val batches = CourseUtils.getActiveBatches(fetchData, batchList, sunbirdCoursesKeyspace)
    val userData = CommonUtil.time({
      CourseUtils.recordTime(CourseUtils.getUserData(spark, fetchData), "Time taken to get generate the userData- ")
    })
    val modelParams = config.modelParams.get
    val filteredBatches = CourseUtils.getFilteredBatches(spark, batches, config)
    val activeBatchesCount = new AtomicInteger(filteredBatches.length)
    metrics.put("userDFLoadTime", userData._1)
    metrics.put("activeBatchesCount", activeBatchesCount.get())
    val batchFilters = JSONUtils.serialize(modelParams("batchFilters"))
    val uploadToAzure = AppConf.getConfig("course.upload.reports.enabled")
    val tempDir = AppConf.getConfig("assessment.metrics.temp.dir")
    val reportPath = modelParams.getOrElse("reportPath", "course-progress-reports/").asInstanceOf[String]
    val assessmentProfileDF = getAssessmentProfileDF(fetchData).persist(StorageLevel.MEMORY_ONLY)
    for (index <- filteredBatches.indices) {
      val row = filteredBatches(index)
      val courses = CourseUtils.getCourseInfo(spark, row.getString(0))
      val batch = CourseBatch(row.getString(1), row.getString(2), row.getString(3), courses.channel);
      if (null != courses.framework && courses.framework.nonEmpty && batchFilters.toLowerCase.contains(courses.framework.toLowerCase)) {
        val result = CommonUtil.time({
          val reportDF = CourseUtils.recordTime(getReportDF(batch, userData._2, assessmentProfileDF, modelParams.getOrElse("applyPrivacyPolicy", true).asInstanceOf[Boolean]), s"Time taken to generate DF for batch ${batch.batchid} - ")
          val contentIds: List[String] = reportDF.select(col("content_id")).distinct().collect().map(_ (0)).toList.asInstanceOf[List[String]]
          if (contentIds.nonEmpty) {
            val denormalizedDF = CourseUtils.recordTime(denormAssessment(reportDF, contentIds.distinct).persist(StorageLevel.MEMORY_ONLY), s"Time take to denorm the assessment - ")
            val totalRecords = denormalizedDF.count()
            if (totalRecords > 0) CourseUtils.recordTime(saveReport(denormalizedDF, uploadToAzure, batch.batchid, reportPath), s"Time take to save the $totalRecords for batch ${batch.batchid} all the reports into azure -")
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


  def getAssessmentProfileDF(fetchData: (SparkSession, Map[String, String], String, Option[StructType], Option[Seq[String]]) => DataFrame)(implicit spark: SparkSession): DataFrame = {
    val userEnrolmentDF = fetchData(spark, Map("table" -> "user_enrolments", "keyspace" -> sunbirdCoursesKeyspace), cassandraUrl, Some(new StructType()), Some(Seq("batchid", "userid", "courseid", "active", "completionpercentage", "enrolleddate", "completedon")))
      .filter(lower(col("active")).equalTo("true")).persist(StorageLevel.MEMORY_ONLY)
    val assessmentProfileDF = fetchData(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace), cassandraUrl, Some(new StructType()), Some(Seq("course_id", "batch_id", "user_id", "content_id", "total_max_score", "total_score", "grand_total")))
    val assessmentDF = getAssessmentData(assessmentProfileDF)

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

  def getReportDF(batch: CourseBatch, userDF: DataFrame, assessmentProfileDF: DataFrame, applyPrivacyPolicy: Boolean)(implicit spark: SparkSession): DataFrame = {
    JobLogger.log("Creating report for batch " + batch.batchid, None, INFO)
    val filteredAssessmentProfileDF = assessmentProfileDF.where(col("batchid") === batch.batchid)
      .withColumn("enddate", lit(batch.endDate))
      .withColumn("startdate", lit(batch.startDate))
      .withColumn("channel", lit(batch.courseChannel))
      .select(col("batchid"),
        col("enddate"),
        col("startdate"),
        col("channel").as("course_channel"),
        col("userid"),
        col("courseid"),
        col("active"),
        col("content_id"),
        col("total_score"),
        col("grand_total"),
        col("total_sum_score")
      )
    val reportDF = filteredAssessmentProfileDF
      .join(userDF, Seq("userid"), "inner")
      .select(filteredAssessmentProfileDF.col("*"),
        col(UserCache.userchannel),
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

    if (applyPrivacyPolicy) {
      reportDF.withColumn(UserCache.externalid, when(reportDF.col("course_channel") === reportDF.col(UserCache.userchannel), reportDF.col(UserCache.externalid)).otherwise(""))
        .withColumn(UserCache.schoolname, when(reportDF.col("course_channel") === reportDF.col(UserCache.userchannel), reportDF.col(UserCache.schoolname)).otherwise(""))
        .withColumn(UserCache.schooludisecode, when(reportDF.col("course_channel") === reportDF.col(UserCache.userchannel), reportDF.col(UserCache.schooludisecode)).otherwise(""))
    } else reportDF
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
    val contentMetaDataDF = CourseUtils.getContentNames(spark, contentIds, AppConf.getConfig("assessment.metrics.supported.contenttype"))
    report.join(contentMetaDataDF, report.col("content_id") === contentMetaDataDF.col("identifier"), "right_outer") // Doing right join since to generate report only for the "SelfAssess" content types
      .select(
        col("name").as("content_name"),
        col("total_sum_score"), report.col(UserCache.userid), report.col("courseid"), report.col("batchid"),
        col("grand_total"), report.col(UserCache.maskedemail), report.col(UserCache.district), report.col(UserCache.maskedphone),
        report.col(UserCache.orgname), report.col(UserCache.externalid), report.col(UserCache.schoolname),
        report.col("username"), col(UserCache.state), col(UserCache.schooludisecode))
  }
  /**
   * This method is used to upload the report the azure cloud service
   */
  def saveReport(reportDF: DataFrame, uploadToAzure: String, batchid: String, reportPath: String)(implicit spark: SparkSession, fc: FrameworkContext): Unit = {
    val storageConfig = getStorageConfig(AppConf.getConfig("cloud.container.reports"), reportPath)
    if (StringUtils.isNotBlank(uploadToAzure) && StringUtils.equalsIgnoreCase("true", uploadToAzure)) {
      val transposedData = transposeDF(reportDF)
      val reportData = transposedData.join(reportDF, Seq("courseid", "batchid", "userid"), "inner")
        .dropDuplicates("userid", "courseid", "batchid").drop("content_name")
      getFinalDF(reportData, finalColumnMapping, finalColumnOrder)
        .saveToBlobStore(storageConfig, "csv", "report-" + batchid, Option(Map("header" -> "true")), None);
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

}