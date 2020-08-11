package org.sunbird.analytics.job.report

import java.util.concurrent.atomic.AtomicInteger

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Encoders, Row, SQLContext, SparkSession}
import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.util.Constants
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat
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
    val batchIds = if(conf.length > 1) {
      conf(1).split(",").toList
    } else List()

    val jobConfig = JSONUtils.deserialize[JobConfig](conf(0))
    JobContext.parallelization = CommonUtil.getParallelization(jobConfig);
    implicit val sparkContext: SparkContext = getReportingSparkContext(jobConfig);
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext();
    execute(jobConfig, batchIds)
  }

  private def execute(config: JobConfig, batchList: List[String])(implicit sc: SparkContext, fc: FrameworkContext) = {
    val tempDir = AppConf.getConfig("assessment.metrics.temp.dir")
    val readConsistencyLevel: String = AppConf.getConfig("assessment.metrics.cassandra.input.consistency")
    val sparkConf = sc.getConf
      .set("spark.cassandra.input.consistency.level", readConsistencyLevel)
      .set("spark.sql.caseSensitive", AppConf.getConfig(key = "spark.sql.caseSensitive"))
    val spark: SparkSession = SparkSession.builder.config(sparkConf).getOrCreate()
    val batchFilters = JSONUtils.serialize(config.modelParams.get("batchFilters"))

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
    * @param url - Cassandra/Redis url
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
    val batches = getBatchList(loadData, batchList)
    val userData = CommonUtil.time({
      recordTime(getUserData(spark, loadData), "Time taken to get generate the userData- ")
    })

    val activeBatchesCount = new AtomicInteger(batches.length)
    metrics.put("userDFLoadTime", userData._1)
    metrics.put("activeBatchesCount", activeBatchesCount.get())
    val batchFilters = JSONUtils.serialize(config.modelParams.get("batchFilters"))
    val uploadToAzure = AppConf.getConfig("course.upload.reports.enabled")
    val tempDir = AppConf.getConfig("assessment.metrics.temp.dir")

    for (index <- batches.indices) {
      val row = batches(index)
      val courses = CourseUtils.getCourseInfo(spark, row.getString(0))
      if(courses.framework.nonEmpty && batchFilters.toLowerCase.contains(courses.framework.toLowerCase)) {
        val batch = CourseBatch(row.getString(1), row.getString(2), row.getString(3), courses.channel);
        val result = CommonUtil.time({
          val reportDF = recordTime(getReportDF(batch, userData._2, loadData), s"Time taken to generate DF for batch ${batch.batchid} - ")
          val denormalizedDF = recordTime(denormAssessment(reportDF), s"Time take to denorm the assessment - ")
          recordTime(saveReport(denormalizedDF, tempDir, uploadToAzure, batch.batchid), s"Time take to save the all the reports into azure -")
        })
        JobLogger.log(s"Time taken to generate report for batch ${batch.batchid} is ${result._1}. Remaining batches - ${activeBatchesCount.getAndDecrement()}", None, INFO)
      }
    }
  }

  def getBatchList(loadData: (SparkSession, Map[String, String], String, StructType) => DataFrame, batchList: List[String])(implicit spark: SparkSession): Array[Row] = {
    val courseBatchDF = if(batchList.nonEmpty) {
      loadData(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace), cassandraUrl, new StructType())
        .filter(batch => batchList.contains(batch.getString(1)))
        .select("courseid", "batchid", "enddate", "startdate")
    }
    else {
      loadData(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace), cassandraUrl, new StructType())
        .select("courseid", "batchid", "enddate", "startdate")
    }
    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
    val comparisonDate = fmt.print(DateTime.now(DateTimeZone.UTC).minusDays(1))

    JobLogger.log("Filtering out inactive batches where date is >= " + comparisonDate, None, INFO)

    val activeBatches = courseBatchDF.filter(col("enddate").isNull || to_date(col("enddate"), "yyyy-MM-dd").geq(lit(comparisonDate)))
    val activeBatchList = activeBatches.select("courseid","batchid", "startdate", "enddate").collect
    JobLogger.log("Total number of active batches:" + activeBatchList.length, None, INFO)

    activeBatchList
  }

  def getUserData(spark: SparkSession, loadData: (SparkSession, Map[String, String], String, StructType) => DataFrame): DataFrame = {
    val schema = Encoders.product[UserData].schema
    val userDf = loadData(spark, Map("table" -> "user","infer.schema" -> "true", "key.column"-> "userid"), "org.apache.spark.sql.redis", schema)
      .withColumn("username",concat_ws(" ", col("firstname"), col("lastname")))
    userDf.persist()
    userDf
  }

  def getReportDF(batch: CourseBatch, userDF: DataFrame, loadData: (SparkSession, Map[String, String], String, StructType) => DataFrame)(implicit spark: SparkSession): DataFrame = {
    JobLogger.log("Creating report for batch " + batch.batchid, None, INFO)

    val userCoursesDF = loadData(spark, Map("table" -> "user_enrolments", "keyspace" -> sunbirdCoursesKeyspace), cassandraUrl, new StructType())
      .filter(lower(col("active")).equalTo("true"))
      .select(col("batchid"), col("userid"), col("courseid"), col("active")
        , col("completionpercentage"), col("enrolleddate"), col("completedon"))
      .where(col("batchid") === batch.batchid)
      .withColumn("enddate", lit(batch.endDate))
      .withColumn("startdate", lit(batch.startDate))
      .withColumn("channel", lit(batch.courseChannel))
      .select(col("batchid"),
        col("enddate"),
        col("startdate"),
        col("channel"),
        col("userid"),
        col("courseid"),
        col("active"))

    val userDenormDF = userCoursesDF
      .join(userDF, Seq("userid"), "inner")
      .withColumn(UserCache.externalid, when(userCoursesDF.col("channel") === userDF.col(UserCache.userchannel), userDF.col(UserCache.externalid)).otherwise(""))
      .withColumn(UserCache.schoolname, when(userCoursesDF.col("channel") === userDF.col(UserCache.userchannel), userDF.col(UserCache.schoolname)).otherwise(""))
      .withColumn(UserCache.block, when(userCoursesDF.col("channel") === userDF.col(UserCache.userchannel), userDF.col(UserCache.block)).otherwise(""))
      .withColumn(UserCache.schooludisecode, when(userCoursesDF.col("channel") === userDF.col(UserCache.userchannel), userDF.col(UserCache.schooludisecode)).otherwise(""))
      .select(userCoursesDF.col("*"),
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
        col("username"))

    val assessmentProfileDF = loadData(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace), cassandraUrl, new StructType())
      .select("course_id", "batch_id", "user_id", "content_id", "total_max_score", "total_score", "grand_total")

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
      .select("batchid", "courseid", UserCache.userid, UserCache.maskedemail, UserCache.maskedphone, "username", UserCache.district,
        UserCache.externalid, UserCache.schoolname, UserCache.schooludisecode, UserCache.state, UserCache.orgname,
        "content_id", "total_score", "grand_total", "total_sum_score")
    userDF.unpersist()
    reportDF
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
    val response = RestUtil.post[CourseResponse](apiUrl,request)
    val assessmentInfo = if (null != response && response.responseCode.equalsIgnoreCase("ok") && response.result.count > 0) {
      response.result.content
    } else List()

    assessmentInfo.toDF().select("name","identifier")
  }

  /**
    * This method is used to upload the report the azure cloud service
    */
  def saveReport(reportDF: DataFrame, url: String, uploadToAzure: String, batchid: String)(implicit spark: SparkSession, fc: FrameworkContext): Unit = {
    if (StringUtils.isNotBlank(uploadToAzure) && StringUtils.equalsIgnoreCase("true", uploadToAzure)) {
      save(reportDF, url, spark, batchid)
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

  def save(reportDF: DataFrame, url: String, spark: SparkSession, batchId: String)(implicit fc: FrameworkContext): Unit = {
    val transposedData = transposeDF(reportDF)
    val reportData = transposedData.join(reportDF, Seq("courseid", "batchid", "userid"), "inner")
      .dropDuplicates("userid", "courseid", "batchid").drop("content_name")
    try {
      val urlBatch: String = recordTime(saveToAzure(reportData, url, batchId, transposedData), s"Time taken to save the $batchId into azure -")
    } catch {
      case e: Exception => JobLogger.log("File upload is failed due to " + e, None, ERROR)
    }
  }

}