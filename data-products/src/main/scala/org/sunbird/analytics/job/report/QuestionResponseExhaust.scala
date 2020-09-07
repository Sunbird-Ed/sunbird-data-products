package org.sunbird.analytics.job.report

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig, JobContext, StorageConfig}
import org.sunbird.analytics.job.report.AssessmentMetricsJobV2.cassandraUrl
import org.sunbird.cloud.storage.conf.AppConf

import scala.collection.mutable

case class AssessmentAggData(course_id: String, batch_id: String, user_id: String, content_id: String, attempt_id: String,
                             created_on: String, grand_total: String, last_attempted_on: String, question: List[QuestionData],
                             total_max_score: String, total_score: String, updated_on: String)
case class QuestionData(id: String, assess_ts: String, max_score: String, score: Double, `type`: String,
                        title: String, resvalues: List[Map[String, String]], params: List[Map[String, String]],
                        description: String, duration: String)

object QuestionResponseExhaust extends optional.Application with BaseReportsJob {

  implicit val className: String = "org.ekstep.analytics.job.QuestionResponseExhaust"
  val sunbirdCoursesKeyspace: String = AppConf.getConfig("course.metrics.cassandra.sunbirdCoursesKeyspace")
  val metrics: mutable.Map[String, BigInt] = mutable.Map[String, BigInt]()
  val baseCourseReport = new BaseCourseReport
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
      prepareReport(spark, storageConfig, baseCourseReport.loadData, config, batchList)
    })
    metrics.put("totalExecutionTime", time._1)
    JobLogger.end("CourseMetrics Job completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name, "metrics" -> metrics)))
    fc.closeContext()
  }

  def prepareReport(spark: SparkSession, storageConfig: StorageConfig, loadData: (SparkSession, Map[String, String], String, StructType) => DataFrame,
                    config: JobConfig, batchIds: List[String]): Unit = {
    val assessmentSchema = Encoders.product[AssessmentAggData].schema
    val assessmentProfileDF = loadData(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace), cassandraUrl, assessmentSchema)
    println("assessmentDF")
    assessmentProfileDF.show()
    val data = assessmentProfileDF.rdd.map{f =>
      println("assessment: " + f)
    }

  }


}
