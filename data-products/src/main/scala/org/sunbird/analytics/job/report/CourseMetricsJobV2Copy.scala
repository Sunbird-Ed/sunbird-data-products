package org.sunbird.analytics.job.report

import java.util.concurrent.atomic.AtomicInteger

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, unix_timestamp, _}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework.Level._
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.sunbird.analytics.job.report.CourseMetricsJobV2.{getReportDF, getStorageConfig, getUserCourseInfo, getUserData, getUserEnrollmentDF, metrics, recordTime, saveReportToBlobStore}
import org.sunbird.analytics.util.{CourseUtils, UserCache, UserData}
import org.sunbird.cloud.storage.conf.AppConf

import scala.collection.mutable




case class ReportLocations(requestId : String, locations :List[String])
case class  ContentBatch(courseId : String, batchId: String, startDate: String, endDate: String)
case class Reports(requestId: String, reportPath: String, batchIds : List[ContentBatch])


object CourseMetricsJobV2Copy extends scala.App with ReportOnDemandModelTemplate[Reports,ReportLocations,ReportLocations] with IJob  with BaseReportsJob  {

  implicit val className: String = "org.ekstep.analytics.job.CourseMetricsJobV2"
  val sunbirdKeyspace: String = AppConf.getConfig("course.metrics.cassandra.sunbirdKeyspace")


  // $COVERAGE-OFF$ Disabling scoverage for main and execute method
  override def name(): String = "CourseMetricsJobV2"

  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
    JobLogger.init("CourseMetricsJob")
    JobLogger.start("CourseMetrics Job Started executing", Option(Map("config" -> config, "model" -> name)))

    val conf = config.split(";")
    val batchIds = if (conf.length > 1) {
      conf(1).split(",").toList
    } else List()
    val jobConfig = JSONUtils.deserialize[JobConfig](conf(0))
    JobContext.parallelization = CommonUtil.getParallelization(jobConfig)

    //implicit val sparkContext: SparkContext = getReportingSparkContext(jobConfig)
      implicit val spark  : SparkSession = openSparkSession(jobConfig)
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()

  }


    override def filterReports(reportConfigs: Dataset[ReportConfigs], config: Map[String, AnyRef])(implicit spark: SparkSession, fc: FrameworkContext): Dataset[Reports] = {

        val sunbirdCoursesKeyspace: String = AppConf.getConfig("course.metrics.cassandra.sunbirdCoursesKeyspace")
        val sunbirdHierarchyStore: String = AppConf.getConfig("course.metrics.cassandra.sunbirdHierarchyStore")
        val metrics: mutable.Map[String, BigInt] = mutable.Map[String, BigInt]()


        import spark.implicits._
        reportConfigs.show(false)
        val batchList =List()
        println(sunbirdCoursesKeyspace)
        val reportEncoder = Encoders.product[Reports]
        val activeBatches = CourseUtils.getActiveBatches(fc.loadData, batchList, sunbirdCoursesKeyspace)
        val filteredReports = reportConfigs.as[ReportConfigs].collect.map(f=>{
            val contentFilters = config.getOrElse("contentFilters", Map()).asInstanceOf[Map[String,AnyRef]]
            val applyPrivacyPolicy = true
            val reportPath = "course-progress-reports/"

            val filteredBatches = if(contentFilters.nonEmpty) {
                val filteredContents = CourseUtils.filterContents(spark, JSONUtils.serialize(contentFilters)).toDF()
                activeBatches.join(filteredContents, activeBatches.col("courseid") === filteredContents.col("identifier"), "inner")
                  .select(activeBatches.col("*")).map(f => ContentBatch(f.getString(0),f.getString(1),f.getString(2),f.getString(3)))collect
            } else {
                println(activeBatches)
                activeBatches.show(false)
                activeBatches.map(f => ContentBatch(f.getString(0),f.getString(1),f.getString(2),f.getString(3))).collect
            }
            Reports(f.requestId,reportPath,filteredBatches.toList)

        })

        spark.createDataset(filteredReports)
    }

    override def generateReports(filteredReports: Dataset[Reports], config: Map[String, AnyRef])(implicit spark: SparkSession, fc: FrameworkContext): Dataset[ReportLocations] = {
        import spark.implicits._
        val finalColumnMapping = Map(UserCache.externalid ->  "External ID", UserCache.userid -> "User ID",
            "username" -> "User Name",UserCache.maskedemail -> "Email ID", UserCache.maskedphone -> "Mobile Number",
            UserCache.orgname -> "Organisation Name", UserCache.state -> "State Name", UserCache.district -> "District Name",
            UserCache.schooludisecode -> "School UDISE Code", UserCache.schoolname -> "School Name", UserCache.block -> "Block Name",
            "enrolleddate" -> "Enrolment Date", "courseid" -> "Course ID", "course_completion" -> "Course Progress",
            "completedon" -> "Completion Date", "certificate_status" -> "Certificate Status")
        val finalColumnOrder = List("External ID","User ID","User Name","Email ID","Mobile Number","Organisation Name",
            "State Name","District Name","School UDISE Code","School Name","Block Name","Enrolment Date","Course ID",
            "Course Progress","Completion Date","Certificate Status")
        implicit val sqlContext: SQLContext = spark.sqlContext
        val container = AppConf.getConfig("cloud.container.reports")
        val objectKey = AppConf.getConfig("course.metrics.cloud.objectKey")
        val storageConfig = getStorageConfig(container, objectKey)
        //val filteredBatches = filteredReports.select("batchIds").rdd.flatMap(r => r(0).asInstanceOf[List[ContentBatch]]).collect()
        //val filteredBatches =   filteredReports.select("batchIds").collectAsList().get(0).asInstanceOf[List[ContentBatch]]
        val userCourses = getUserCourseInfo(fc.loadData).persist(StorageLevel.MEMORY_ONLY)
        val userData = CommonUtil.time({
            recordTime(getUserData(spark, fc.loadData), "Time taken to get generate the userData: ")
        })
        val applyPrivacyPolicy =true
        val reportPath= "course-report-path/"
        //val activeBatchesCount = new AtomicInteger(filteredBatches.length)
        //metrics.put("userDFLoadTime", userData._1)
        //metrics.put("activeBatchesCount", activeBatchesCount.get())
        val batchFilters = JSONUtils.serialize("[\"TBD\"]")
        val userEnrolmentDF = getUserEnrollmentDF(fc.loadData).persist(StorageLevel.MEMORY_ONLY)

        val userCourseData = userCourses.join(userData._2, userCourses.col("userid") === userData._2.col("userid"), "inner")
          .select(userData._2.col("*"),
              userCourses.col("courseid"),
              userCourses.col("contextid"),
              userCourses.col("completionPercentage").as("course_completion"),
              userCourses.col("l1identifier"),
              userCourses.col("l1completionPercentage"))
        userCourseData.persist(StorageLevel.MEMORY_ONLY)
        filteredReports.flatMap(f=>{
            val files = f.batchIds.map(row=>{

                val courses = CourseUtils.getCourseInfo(spark, row.courseId)
                val batch = CourseBatch(row.batchId, row.startDate, row.endDate, courses.channel);
                    val result = CommonUtil.time({
                        val reportDF = recordTime(getReportDF(batch, userCourseData, userEnrolmentDF, applyPrivacyPolicy), s"Time taken to generate DF for batch ${batch.batchid} : ")
                        val totalRecords = reportDF.count()
                        val files=saveReportToBlobStore(batch, reportDF, storageConfig, totalRecords, reportPath)
                        reportDF.unpersist(true)
                        ReportLocations(f.requestId,files)
                    })
                result._2
            })
        files
        })

    }

    override def saveReports(generatedreports: Dataset[ReportLocations], config: Map[String, AnyRef])(implicit spark: SparkSession, fc: FrameworkContext): Dataset[ReportLocations] = {
        generatedreports
    }


}
