package org.sunbird.analytics.job.report

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Encoders, SQLContext, SparkSession}
import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger, RestUtil}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.sunbird.analytics.util.{Constants, CourseResponse, CourseUtils}
import org.sunbird.cloud.storage.conf.AppConf

case class CourseInfo(courseid: String, batchid: String, startdate: String, enddate: String, channel: String)
case class CourseBatchOutput(courseid: String, batchid: String, startdate: String, enddate: String)

object AssessmentMetricsJobV2 extends optional.Application with IJob with BaseReportsJob {

  implicit val className = "org.ekstep.analytics.job.AssessmentMetricsJob"

  private val indexName: String = AppConf.getConfig("assessment.metrics.es.index.prefix") + DateTimeFormat.forPattern("dd-MM-yyyy-HH-mm").print(DateTime.now())
  val metrics = scala.collection.mutable.Map[String, BigInt]();
  val sunbirdKeyspace = AppConf.getConfig("course.metrics.cassandra.sunbirdKeyspace")

// $COVERAGE-OFF$ Disabling scoverage for main and execute method
  def name(): String = "AssessmentMetricsJob"

  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
    JobLogger.init("Assessment Metrics")
    JobLogger.start("Assessment Job Started executing", Option(Map("config" -> config, "model" -> name)))
    val jobConfig = JSONUtils.deserialize[JobConfig](config)
    JobContext.parallelization = CommonUtil.getParallelization(jobConfig);
    implicit val sparkContext: SparkContext = getReportingSparkContext(jobConfig);
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext();
    execute(jobConfig)
  }

  private def execute(config: JobConfig)(implicit sc: SparkContext, fc: FrameworkContext) = {
    val tempDir = AppConf.getConfig("assessment.metrics.temp.dir")
    val readConsistencyLevel: String = AppConf.getConfig("assessment.metrics.cassandra.input.consistency")
    val sparkConf = sc.getConf
      .set("spark.cassandra.input.consistency.level", readConsistencyLevel)
      .set("spark.sql.caseSensitive", AppConf.getConfig(key = "spark.sql.caseSensitive"))
    implicit val spark: SparkSession = SparkSession.builder.config(sparkConf).getOrCreate()
    val batchFilters = JSONUtils.serialize(config.modelParams.get("batchFilters"))
    val time = CommonUtil.time({
      val reportDF = recordTime(prepareReport(spark, loadData, batchFilters).cache(), s"Time take generate the dataframe} - ")
      val denormalizedDF = recordTime(denormAssessment(reportDF), s"Time take to denorm the assessment - ")
      val uploadToAzure = AppConf.getConfig("course.upload.reports.enabled")
      recordTime(saveReport(denormalizedDF, tempDir, uploadToAzure), s"Time take to save the all the reports into both azure and es -")
      reportDF.unpersist(true)
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
   * @param url - Cassandra/Redis url
   * @return
   */
  def loadData(spark: SparkSession, settings: Map[String, String], url: String): DataFrame = {
    spark
      .read
      .format(url)
      .options(settings)
      .load()
  }

  /**
   * Loading the specific tables from the cassandra db.
   */
  def prepareReport(spark: SparkSession, loadData: (SparkSession, Map[String, String], String) => DataFrame, batchFilters: String)(implicit fc: FrameworkContext): DataFrame = {
    val sunbirdCoursesKeyspace = AppConf.getConfig("course.metrics.cassandra.sunbirdCoursesKeyspace")
    val cassandraUrl = "org.apache.spark.sql.cassandra"
    val courseBatchDF = loadData(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace), cassandraUrl).select("courseid", "batchid", "startdate", "enddate")
    val userCoursesDF = loadData(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace), cassandraUrl)
      .filter(lower(col("active")).equalTo("true"))
      .select(col("batchid"), col("userid"), col("courseid"), col("active")
        , col("completionpercentage"), col("enrolleddate"), col("completedon"))

    val userDF = loadData(spark, Map("keys.pattern" -> "*","infer.schema" -> "true"), "org.apache.spark.sql.redis")
      .select(col("userid"),col("firstname"),col("lastname"),col("maskedemail"),col("maskedphone"),
        col("districtname"), col("externalid"),col("schoolname"),col("schooludisecode"),col("statename"),col("orgname"),
        concat_ws(" ", col("firstname"), col("lastname")).as("username"))

    val assessmentProfileDF = loadData(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace), cassandraUrl)
      .select("course_id", "batch_id", "user_id", "content_id", "total_max_score", "total_score", "grand_total")

    implicit val sqlContext = new SQLContext(spark.sparkContext)
    import sqlContext.implicits._

    val encoder = Encoders.product[CourseBatchOutput]
    val courseBatchRdd = courseBatchDF.as[CourseBatchOutput](encoder).rdd

    val courseChannelDenormDF = courseBatchRdd.map(f => {
      val courses = CourseUtils.getCourseInfo(spark, f.courseid)
      if(courses.framework.nonEmpty && batchFilters.toLowerCase.contains(courses.framework.toLowerCase)) {
        CourseInfo(f.courseid,f.batchid,f.startdate,f.enddate,courses.channel)
      }
      else CourseInfo("","","","","")
    }).filter(f => f.courseid.nonEmpty).toDF()

    /*
   * courseBatchDF has details about the course and batch details for which we have to prepare the report
   * courseBatchDF is the primary source for the report
   * userCourseDF has details about the user details enrolled for a particular course/batch
   * */

    val userCourseDenormDF = courseChannelDenormDF.join(userCoursesDF, userCoursesDF.col("batchid") === courseChannelDenormDF.col("batchid"), "inner")
      .select(
        userCoursesDF.col("batchid"),
        col("userid"),
        col("active"),
        courseChannelDenormDF.col("courseid"),
        courseChannelDenormDF.col("channel").as("course_channel"))
    /*
  *userCourseDenormDF lacks some of the user information that need to be part of the report
  *here, it will add some more user details
  * */

    val userDenormDF = userCourseDenormDF
      .join(userDF, userDF.col("userid") === userCourseDenormDF.col("userid"), "inner")
      .select(
        userCourseDenormDF.col("courseid"),
        userCourseDenormDF.col("batchid"),
        userCourseDenormDF.col("active"),
        userCourseDenormDF.col("course_channel"),
        userDF.col("*"))

    val assessmentDF = getAssessmentData(assessmentProfileDF)
    /**
     * Compute the sum of all the worksheet contents score.
     */
    val assessmentAggDf = Window.partitionBy("user_id", "batch_id", "course_id")
    val resDF = assessmentDF
      .withColumn("agg_score", sum("total_score") over assessmentAggDf)
      .withColumn("agg_max_score", sum("total_max_score") over assessmentAggDf)
      .withColumn("total_sum_score", concat(ceil((col("agg_score") * 100) / col("agg_max_score")), lit("%")))
    /**
     * Filter only valid enrolled userid for the specific courseid
     */

    val reportDF = userDenormDF.join(resDF,
      userDenormDF.col("userid") === resDF.col("user_id")
        && userDenormDF.col("batchid") === resDF.col("batch_id")
        && userDenormDF.col("courseid") === resDF.col("course_id"), "inner")
      .select("batchid", "courseid", "userid", "maskedemail", "maskedphone", "username", "districtname",
        "externalid", "schoolname", "schooludisecode", "statename", "orgname",
        "content_id", "total_score", "grand_total", "total_sum_score")

    userDF.unpersist()
    reportDF
  }

  /**
   * De-norming the assessment report - Adding content name column to the content id
   *
   * @return - Assessment denormalised dataframe
   */
  def denormAssessment(report: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val contentIds: List[String] = recordTime(report.select(col("content_id")).distinct().collect().map(_ (0)).toList.asInstanceOf[List[String]], "Time taken to get the content IDs- ")
    JobLogger.log("ContentIds are" + contentIds, None, INFO)
    val contentMetaDataDF = getAssessmentNames(spark, contentIds, AppConf.getConfig("assessment.metrics.supported.contenttype"))
    report.join(contentMetaDataDF, report.col("content_id") === contentMetaDataDF.col("identifier"), "right_outer") // Doing right join since to generate report only for the "SelfAssess" content types
      .select(
        col("name").as("content_name"),
        col("total_sum_score"), report.col("userid"), report.col("courseid"), report.col("batchid"),
        col("grand_total"), report.col("maskedemail"), report.col("districtname"), report.col("maskedphone"),
        report.col("orgname"), report.col("externalid"), report.col("schoolname"),
        report.col("username"), col("statename"), col("schooludisecode"))
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
    val response = RestUtil.post[CourseResponse](apiUrl,request)
    val assessmentInfo = if (null != response && response.responseCode.equalsIgnoreCase("ok") && response.result.count > 0) {
      response.result.content
    } else List()

    assessmentInfo.toDF().select("name","identifier")
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
    reportDF.withColumn("rownum", row_number.over(df)).where(col("rownum") === 1).drop("rownum")
  }


  /**
   * This method is used to upload the report the azure cloud service and
   * Index report data into core elastic search.
   * Alias name: cbatch-assessment
   * Index name: cbatch-assessment-24-08-1993-09-30 (dd-mm-yyyy-hh-mm)
   */
  def saveReport(reportDF: DataFrame, url: String, uploadToAzure: String)(implicit spark: SparkSession, fc: FrameworkContext): Unit = {
    val result = reportDF.groupBy("courseid").agg(collect_list("batchid").as("batchid"))
    if (StringUtils.isNotBlank(uploadToAzure) && StringUtils.equalsIgnoreCase("true", uploadToAzure)) {
      val courseBatchList = result.collect.map(r => Map(result.columns.zip(r.toSeq): _*))
      save(courseBatchList, reportDF, url, spark)
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

  def saveToAzure(reportDF: DataFrame, url: String, batchId: String, transposedData: DataFrame): String = {
    val tempDir = AppConf.getConfig("assessment.metrics.temp.dir")
    val storageConfig = getStorageConfig(AppConf.getConfig("cloud.container.reports"), AppConf.getConfig("assessment.metrics.cloud.objectKey"))
    val azureData = reportDF.select(
      reportDF.col("externalid").as("External ID"),
      reportDF.col("userid").as("User ID"),
      reportDF.col("username").as("User Name"),
      reportDF.col("maskedemail").as("Email ID"),
      reportDF.col("maskedphone").as("Mobile Number"),
      reportDF.col("orgname").as("Organisation Name"),
      reportDF.col("statename").as("State Name"),
      reportDF.col("districtname").as("District Name"),
      reportDF.col("schooludisecode").as("School UDISE Code"),
      reportDF.col("schoolname").as("School Name"),
      transposedData.col("*"), // Since we don't know the content name column so we are using col("*")
      reportDF.col("total_sum_score").as("Total Score"))
      .drop("userid", "courseid", "batchid")
    azureData.saveToBlobStore(storageConfig, "csv", "report-" + batchId, Option(Map("header" -> "true")), None);
    s"${AppConf.getConfig("cloud.container.reports")}/${AppConf.getConfig("assessment.metrics.cloud.objectKey")}/report-$batchId.csv"

  }

  def save(courseBatchList: Array[Map[String, Any]], reportDF: DataFrame, url: String, spark: SparkSession)(implicit fc: FrameworkContext): Unit = {
    courseBatchList.foreach(item => {
      val courseId = item.getOrElse("courseid", "").asInstanceOf[String]
      val batchList = item.getOrElse("batchid", "").asInstanceOf[Seq[String]].distinct
      JobLogger.log(s"Course batch mappings- courseId: $courseId and batchIdList is $batchList ", None, INFO)
      batchList.foreach(batchId => {
        if (!courseId.isEmpty && !batchId.isEmpty) {
          val filteredDF = reportDF.filter(col("courseid") === courseId && col("batchid") === batchId)
          val transposedData = transposeDF(filteredDF)
          val reportData = transposedData.join(reportDF, Seq("courseid", "batchid", "userid"), "inner")
            .dropDuplicates("userid", "courseid", "batchid").drop("content_name")
          try {
            val urlBatch: String = recordTime(saveToAzure(reportData, url, batchId, transposedData), s"Time taken to save the $batchId into azure -")
          } catch {
            case e: Exception => JobLogger.log("File upload is failed due to " + e, None, ERROR)
          }
        } else {
          JobLogger.log("Report failed to create since course_id is " + courseId + "and batch_id is " + batchId, None, ERROR)
        }
      })
    })
  }

}