package org.sunbird.analytics.job.report

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.sunbird.analytics.job.report.CourseMetricsJobV2.{getReportDF, getUserCourseInfo, getUserData, getUserEnrollmentDF, recordTime, saveReportToBlobStore}
import org.sunbird.analytics.util.CourseUtils
import org.sunbird.cloud.storage.conf.AppConf

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


case class ContentBatch(courseId: String, batchId: String, startDate: String, endDate: String)

case class Reports(requestId: String, reportPath: String, batchIds: List[ContentBatch], count: Long)


object CourseMetricsJobV2Copy extends scala.App with ReportOnDemandModelTemplate[Reports, OnDemandJobRequest] with IJob with BaseReportsJob {

    implicit val className: String = "org.ekstep.analytics.job.CourseMetricsJobV2"

    // $COVERAGE-OFF$ Disabling scoverage for main and execute method

    val metrics: mutable.Map[String, BigInt] = mutable.Map[String, BigInt]()

    override def name(): String = "CourseMetricsJobV2"

    def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
        JobLogger.init("CourseMetricsJob")
        JobLogger.start("CourseMetrics Job Started executing", Option(Map("config" -> config, "model" -> name)))

        val jobConfig = JSONUtils.deserialize[JobConfig](config)
        JobContext.parallelization = CommonUtil.getParallelization(jobConfig)
        implicit val spark: SparkSession = openSparkSession(jobConfig)
        implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()


        execute(Some(JSONUtils.deserialize[Map[String, AnyRef]](config)))
    }

    override def filterReports(reportConfigs: Dataset[OnDemandJobRequest], config: Map[String, AnyRef])(implicit spark: SparkSession, fc: FrameworkContext): Dataset[Reports] = {

        import spark.implicits._
        val sunbirdCoursesKeyspace: String = AppConf.getConfig("course.metrics.cassandra.sunbirdCoursesKeyspace")
        val batchList = List()
        val activeBatches = CourseUtils.getActiveBatches(fc.loadData, batchList, sunbirdCoursesKeyspace)
        val filteredReports = reportConfigs.as[OnDemandJobRequest].collect.map(f => {
            val request_data = JSONUtils.deserialize[Map[String, AnyRef]](f.request_data)
            val contentFilters = config.getOrElse("contentFilters", Map()).asInstanceOf[Map[String, AnyRef]]
            val filteredBatches = if (contentFilters.nonEmpty) {
                val filteredContents = CourseUtils.filterContents(spark, JSONUtils.serialize(contentFilters)).toDF()
                activeBatches.join(filteredContents, activeBatches.col("courseid") === filteredContents.col("identifier"), "inner")
                  .select(activeBatches.col("*"))
                  .map(f => ContentBatch(f.getString(0), f.getString(1), f.getString(2), f.getString(3))).collect
            } else {

                activeBatches.map(f => ContentBatch(f.getString(0), f.getString(1), f.getString(2), f.getString(3))).collect
            }
            Reports(f.request_id, request_data.getOrElse("reportPath", "").asInstanceOf[String], filteredBatches.toList, filteredBatches.length)

        })
        spark.createDataset(filteredReports)
    }

    override def generateReports(filteredReports: Dataset[Reports], config: Map[String, AnyRef])(implicit spark: SparkSession, fc: FrameworkContext): Dataset[OnDemandJobRequest] = {
        import spark.implicits._
        val metrics: mutable.Map[String, BigInt] = mutable.Map[String, BigInt]()
        implicit val sqlContext: SQLContext = spark.sqlContext
        val container = AppConf.getConfig("cloud.container.reports")
        val objectKey = AppConf.getConfig("course.metrics.cloud.objectKey")
        val storageConfig = getStorageConfig(container, objectKey)
        val userCourses = getUserCourseInfo(fc.loadData).persist(StorageLevel.MEMORY_ONLY)
        val userData = CommonUtil.time({
            recordTime(getUserData(spark, fc.loadData), "Time taken to get generate the userData: ")
        })
        val applyPrivacyPolicy = true

        metrics.put("userDFLoadTime", userData._1)
        val userEnrolmentDF = getUserEnrollmentDF(fc.loadData).persist(StorageLevel.MEMORY_ONLY)

        val userCourseData = userCourses.join(userData._2, userCourses.col("userid") === userData._2.col("userid"), "inner")
          .select(userData._2.col("*"),
              userCourses.col("courseid"),
              userCourses.col("contextid"),
              userCourses.col("completionPercentage").as("course_completion"),
              userCourses.col("l1identifier"),
              userCourses.col("l1completionPercentage"))
        userCourseData.persist(StorageLevel.MEMORY_ONLY)
        val reportPaths = filteredReports.collect().map(f => {
            val files = f.batchIds.flatMap(row => {
                var filesPaths = ListBuffer[String]()
                metrics.put(f.requestId + " : activeBatchesCount : ", f.count)
                val courses = CourseUtils.getCourseInfo(spark, row.courseId)
                val batch = CourseBatch(row.batchId, row.startDate, row.endDate, courses.channel)
                val result = CommonUtil.time({
                    val reportDF = recordTime(getReportDF(batch, userCourseData, userEnrolmentDF, applyPrivacyPolicy), s"Time taken to generate DF for batch ${batch.batchid} : ")
                    val totalRecords = reportDF.count()
                    filesPaths = filesPaths ++ saveReportToBlobStore(batch, reportDF, storageConfig, totalRecords, f.reportPath + "/" + f.requestId + "/")
                    reportDF.unpersist(true)
                    filesPaths
                })
                result._2
            })
            OnDemandJobRequest(f.requestId, "",files,"COMPLETED")
        })
        spark.createDataset(reportPaths)

    }

    override def saveReports(reportPaths: Dataset[OnDemandJobRequest], config: Map[String, AnyRef])(implicit spark: SparkSession, fc: FrameworkContext): Dataset[OnDemandJobRequest] = {
        reportPaths
    }
}
