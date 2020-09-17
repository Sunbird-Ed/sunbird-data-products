package org.sunbird.analytics.job.report

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Encoders, SQLContext, SparkSession}
import org.ekstep.analytics.framework.Level._
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig, JobContext, StorageConfig}
import org.sunbird.analytics.util.Constants
import org.sunbird.cloud.storage.conf.AppConf

import scala.collection.mutable

case class AssessmentAggData(course_id: String, batch_id: String, user_id: String, content_id: String, attempt_id: String,
                             created_on: String, grand_total: String, last_attempted_on: String, question: List[QuestionData],
                             total_max_score: String, total_score: String, updated_on: String)
case class QuestionData(id: String, max_score: Double, score: Double, `type`: String,
                        title: String, resvalues: List[Map[String, String]], params: List[Map[String, String]],
                        description: String, duration: String)
case class CourseResponse(result: CourseResult, responseCode: String)
case class CourseResult(count: Int, content: List[CourseBatchInfo])
case class CourseBatchInfo(identifier: String, name: String, batches: List[BatchInfo])
case class BatchInfo(batchId: String, name: String)
case class CourseBatchName(courseId: String, batchId: String)


object QuestionResponseExhaust extends optional.Application with BaseReportsJob {

  implicit val className: String = "org.ekstep.analytics.job.QuestionResponseExhaust"
  val sunbirdCoursesKeyspace: String = AppConf.getConfig("course.metrics.cassandra.sunbirdCoursesKeyspace")
  val metrics: mutable.Map[String, BigInt] = mutable.Map[String, BigInt]()
  val baseCourseReport = new BaseCourseReport
  def name(): String = "CourseMetricsJobV2"
  val cassandraUrl = "org.apache.spark.sql.cassandra"


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
      prepareReport(spark, storageConfig, baseCourseReport.loadData, config, batchList)
    })
    metrics.put("totalExecutionTime", time._1)
    JobLogger.end("CourseMetrics Job completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name, "metrics" -> metrics)))
    fc.closeContext()
  }

  def prepareReport(spark: SparkSession, storageConfig: StorageConfig, loadData: (SparkSession, Map[String, String], String, StructType) => DataFrame,
                    config: JobConfig, batchIds: List[String])(implicit fc: FrameworkContext): Unit = {
    implicit val sparkSession: SparkSession = spark

    implicit val sqlContext: SQLContext = spark.sqlContext
    import sqlContext.implicits._

    val modelParams = config.modelParams.get
    val reportPath = modelParams.getOrElse("reportPath","question-exhaust-reports/").asInstanceOf[String]

    val questionReportDF = getQuestionReport(spark, loadData)
    val courseBatchName = getCourseBatchName(JSONUtils.serialize(config.modelParams.get("esConfig"))).toDF()
      .withColumn("batchInfo", explode(col("batches")))
      .withColumn("batchId", col("batchInfo.batchId"))
      .withColumn("batchName", col("batchInfo.name"))
      .withColumnRenamed("identifier", "courseId")
      .withColumnRenamed("name", "courseName")
      .drop("batchInfo", "batches")
    val courseBatchList = courseBatchName.collect()

    val batchCount = new AtomicInteger(courseBatchList.length)

    for (index <- courseBatchList.indices) {
    val row = courseBatchList(index)
      val reportDF = getReportDF(courseBatchName, questionReportDF, row.getString(2))
      val totalRecords = reportDF.count()
      if (totalRecords > 0) {
        saveReportToBlobStore(row.getString(2), reportDF, storageConfig, totalRecords, reportPath)
      }
    }
  }

  def getReportDF(courseBatchInfo: DataFrame, questionDF: DataFrame, batchId: String) = {
    JobLogger.log("Creating report for batch " + batchId, None, INFO)

    val reportDF = questionDF.where(col("batch_id") === batchId)
      .join(courseBatchInfo, courseBatchInfo.col("batchId") === questionDF.col("batch_id"), "inner")
      .select(
        col("course_id").as("Collection ID"),
        col("courseName").as("Course Name"),
        col("batch_id").as("Batch ID"),
        col("batchName").as("Batch Name"),
        col("user_id").as("DIKSHA UUID"),
        col("attempt_id").as("Attempt Id"),
        col("last_attempted_on").as("Attempted On"),
        col("questionid").as("Question ID"),
        col("questiontype").as("Question Type"),
        col("questiontitle").as("Question Title"),
        col("questiondescription").as("Question Description"),
        col("questionduration").as("Question Duration"),
        col("questionscore").as("Question Score"),
        col("questionmaxscore").as("Question Max Score"),
        col("questionoption").as("Question Options"),
        col("questionresponse").as("Question Response")
      )
    reportDF
  }

  def getCourseBatchName(query: String): List[CourseBatchInfo] = {
    val apiUrl = Constants.COMPOSITE_SEARCH_URL
    val response = RestUtil.post[CourseResponse](apiUrl, query)
    if (null != response && response.responseCode.equalsIgnoreCase("ok")
              && null != response.result.content && response.result.content.nonEmpty) {
      response.result.content
    } else List[CourseBatchInfo]()
  }

  def getQuestionReport(spark: SparkSession, loadData: (SparkSession, Map[String, String], String, StructType) => DataFrame): DataFrame = {
    val assessmentSchema = Encoders.product[AssessmentAggData].schema
    val assessmentProfileDF = loadData(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace), cassandraUrl, assessmentSchema)

    val questionDF = assessmentProfileDF
      .withColumn("questiondata",explode_outer(col("question")) )
      .withColumn("questionid" , col("questiondata.id"))
      .withColumn("questiontype", col("questiondata.type"))
      .withColumn("questiontitle", col("questiondata.title"))
      .withColumn("questiondescription", col("questiondata.description"))
      .withColumn("questionduration", col("questiondata.duration"))
      .withColumn("questionscore", col("questiondata.score"))
      .withColumn("questionmaxscore", col("questiondata.max_score"))
      .withColumn("questionresponse", col("questiondata.resvalues"))
      .withColumn("questionoption", col("questiondata.params"))
      .drop("question", "questiondata")
    questionDF
  }

  def saveReportToBlobStore(batchid: String, reportDF: DataFrame, storageConfig: StorageConfig, totalRecords: Long, reportPath: String): Unit = {
    reportDF
      .saveToBlobStore(storageConfig, "csv", reportPath + "report-" + batchid, Option(Map("header" -> "true")), None)
    JobLogger.log(s"CourseMetricsJob: records stats before cloud upload: { batchId: ${batchid}, totalNoOfRecords: $totalRecords }} ", None, INFO)
  }


}
