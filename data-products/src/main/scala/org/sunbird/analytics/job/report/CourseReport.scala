package org.sunbird.analytics.job.report

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.ekstep.analytics.framework._
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.sunbird.analytics.util.{CourseBatchInfo, CourseUtils, UserCache, UserData}
import org.sunbird.cloud.storage.conf.AppConf


import scala.collection.mutable.ListBuffer



case class CourseBatchMap(batchid: String, startDate: String, endDate: String, courseChannel: String, courseName: String, batchName: String)

case class ContentBatch(courseId: String, batchId: String, startDate: String, endDate: String)

case class Reports(requestId: String, reportPath: String, batchIds: List[ContentBatch], count: Long, batchFilter: List[String], requestedChannel:String)

object CourseReport extends scala.App with ReportOnDemandModelTemplate[Reports, OnDemandJobRequest] with IJob with BaseReportsJob {

  implicit val className: String = "org.ekstep.analytics.job.CourseReportJob"

  override def name(): String = "CourseReportJob"

  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
    JobLogger.init("CourseReportJOb")
    JobLogger.start("CourseReport Job Started executing", Option(Map("config" -> config, "model" -> name)))
    val jobConfig = JSONUtils.deserialize[JobConfig](config)
    JobContext.parallelization = CommonUtil.getParallelization(jobConfig)
    implicit val spark: SparkSession = openSparkSession(jobConfig)
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()
    execute(Some(JSONUtils.deserialize[Map[String, AnyRef]](config)))
  }

  override def filterReports(reportConfigs: Dataset[OnDemandJobRequest], config: Map[String, AnyRef])(implicit spark: SparkSession, fc: FrameworkContext): Dataset[Reports] = {
    import spark.implicits._
    val courseBatches = CourseUtils.loadCourseBatch(fc.loadData, cassandraUrl, List(), sunbirdCoursesKeyspace)
    val filteredReports = reportConfigs.as[OnDemandJobRequest].collect.map(f => {
      val requestObject = JSONUtils.deserialize[Map[String, AnyRef]](f.request_data)
      val contentFilters = requestObject.getOrElse("contentFilters", Map()).asInstanceOf[Map[String, AnyRef]]
      val batchFilters = requestObject.getOrElse("batchFilters", List()).asInstanceOf[List[String]]
      val requestedChannel = requestObject.getOrElse("channel", "").asInstanceOf[String]
      val filteredBatches = if (contentFilters.nonEmpty) {
        val filteredContents = CourseUtils.filterContents(JSONUtils.serialize(contentFilters)).toDF()
        courseBatches.join(filteredContents, courseBatches.col("courseid") === filteredContents.col("identifier"), "inner")
          .select(courseBatches.col("*")).map(f => ContentBatch(f.getString(0), f.getString(1), f.getString(2), f.getString(3))).collect
      } else {
        courseBatches.map(f => ContentBatch(f.getString(0), f.getString(1), f.getString(2), f.getString(3))).collect
      }
      Reports(f.request_id, requestObject.getOrElse("reportPath", "").asInstanceOf[String], filteredBatches.toList, filteredBatches.length, batchFilters, requestedChannel)
    })
    spark.createDataset(filteredReports)

  }

  override def generateReports(filteredReports: Dataset[Reports], config: Map[String, AnyRef])(implicit spark: SparkSession, fc: FrameworkContext): Dataset[OnDemandJobRequest] = {
    import spark.implicits._
    val assessmentDF = getAssessmentData(spark, loadData = fc.loadData)
    val userEnrolmentDF = getUserEnrollment(spark, loadData = fc.loadData)
    val userDF = CourseUtils.getUserData(spark, loadData = fc.loadData, Seq("firstname", "lastname", "userid", "state", "district", "userchannel"))
    val currentDate = DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now(DateTimeZone.UTC).minusDays(1))
    val userCourseInfoDF = getUserCourseInfo(fc.loadData).join(userDF, Seq("userid"), "inner").persist(StorageLevel.MEMORY_ONLY)
    val reportPaths = filteredReports.collect().map(f => {
      val blobPathList = f.batchIds.flatMap(row => {
        var filesPaths = ListBuffer[String]()
        metrics.put(f.requestId + " : TotalBatches : ", f.count)
        val coursesBatchInfo: CourseBatchInfo = CourseUtils.getCourseInfo(spark, row.courseId)
        val batch: CourseBatchMap = CourseBatchMap(row.batchId, row.startDate, row.endDate, coursesBatchInfo.channel, coursesBatchInfo.name, coursesBatchInfo.batches.find(x => row.batchId == x.batchId).map(x => x.name).getOrElse(""));
        val result = CommonUtil.time({
          if (null != coursesBatchInfo.framework && coursesBatchInfo.framework.nonEmpty && f.batchFilter.contains(coursesBatchInfo.framework)) {
            val reportDF = getCourseReport(batch = batch, userCourseInfoDF, userEnrolmentDF, assessmentDF, requestedChannel = f.requestedChannel)
            filesPaths = filesPaths ++ reportDF.saveToBlobStore(getStorageConfig(AppConf.getConfig("cloud.container.reports"), AppConf.getConfig("course.metrics.cloud.objectKey")), "csv", f.reportPath + s"report-${batch.batchid}-progress-${currentDate}", Option(Map("header" -> "true")), None)
            reportDF.unpersist(true)
            filesPaths
          } else {
            JobLogger.log(s"Constrains are not matching, skipping the requestId: ${f.requestId}, batchId: ${batch.batchid} and Remaining batches", None, INFO)
            filesPaths
          }
        })
        result._2
      })
      OnDemandJobRequest(f.requestId, "", blobPathList, "COMPLETED")
    })
    spark.createDataset(reportPaths)
  }


  def getCourseReport(batch: CourseBatchMap, userCourseDF: DataFrame, userEnrollmentDF: DataFrame, assessmentProfileDF: DataFrame, requestedChannel: String)(implicit spark: SparkSession): DataFrame = {
    val columnsOrder = List("Batch Id", "Batch Name", "Collection Id", "Collection Name", "DIKSHA UUID", "User Name", "State", "District", "Enrolment Date", "Completion Date", "Certificate Status", "Course Progress", "Total Score")
    val reportFieldMapping = Map("courseid" -> "Collection Id", "courseName" -> "Collection Name", "batchid" -> "Batch Id", "batchName" -> "Batch Name", "userid" -> "DIKSHA UUID", "user_name" -> "User Name",
      "state" -> "State", "district" -> "District", "enrolleddate" -> "Enrolment Date", "completedon" -> "Completion Date", "course_completion" -> "Course Progress", "total_sum_score" -> "Total Score", "certificate_status" -> "Certificate Status"
    )
    val enrolledUsersToBatch = userEnrollmentDF.where(col("batchid") === batch.batchid)
      .withColumn("batchName", lit(batch.batchName))
      .withColumn("courseName", lit(batch.courseName))
      .withColumn("enddate", lit(batch.endDate))
      .withColumn("startdate", lit(batch.startDate))
      .select(col("batchid"), col("userid"),
        col("enrolleddate"), col("completedon"),
        col("courseid"), col("certificate_status"),
        col("courseName"), col("batchName")
      )
    val reportDF = enrolledUsersToBatch
      .join(userCourseDF, userCourseDF.col("contextid") === s"cb:${batch.batchid}" &&
        enrolledUsersToBatch.col("courseid") === userCourseDF.col("courseid") &&
        enrolledUsersToBatch.col("userid") === userCourseDF.col("userid"), "inner")
      .select(enrolledUsersToBatch.col("*"), col(UserCache.district), col(UserCache.state), col("completionPercentage").as("course_completion"),
        col("l1identifier"), col("l1completionPercentage"), col("user_name"),
          col("userchannel")
      ).persist(StorageLevel.MEMORY_ONLY)

    val assessmentAggDf = Window.partitionBy("userid", "batchid", "courseid")
    val assessDF = assessmentProfileDF
      .withColumn("agg_score", sum("total_score") over assessmentAggDf)
      .withColumn("agg_max_score", sum("total_max_score") over assessmentAggDf)
      .withColumn("total_sum_score", concat(ceil((col("agg_score") * 100) / col("agg_max_score")), lit("%")))

    val assessmentDF = reportDF.join(assessDF, Seq("courseid", "batchid", "userid"), "left_outer")
    val contentIds: List[String] = assessmentDF.select(col("content_id")).distinct().collect().map(_ (0)).toList.asInstanceOf[List[String]]
    val denormedDF = denormAssessment(assessmentDF, contentIds.distinct).persist(StorageLevel.MEMORY_ONLY)
      .select("courseid", "batchid", "userid", "user_name", "district", "state", "course_completion", "l1identifier", "l1completionPercentage", "content_id", "name", "grand_total", "total_sum_score", "batchName", "courseName", "certificate_status", "enrolleddate", "completedon", "userchannel")

    val groupedDF = denormedDF.groupBy("courseid", "batchid", "userid")

    val reportData = transposeDF(groupedDF).join(denormedDF, Seq("courseid", "batchid", "userid"), "inner")
      .dropDuplicates("userid", "courseid", "batchid")
      .drop("content_name", "null", "grand_total", "l1identifier", "l1completionPercentage", "name", "content_id")
    customizeDF(filterReportByConsent(requestedChannel, batch.courseChannel, reportData), reportFieldMapping, columnsOrder)
  }

  def transposeDF(reportDF: RelationalGroupedDataset): DataFrame = {
    val assessment = reportDF.pivot("name").agg(concat(ceil((split(first("grand_total"), "\\/")
      .getItem(0) * 100) / (split(first("grand_total"), "\\/")
      .getItem(1))), lit("%")))
    val leafNodes = reportDF.pivot(concat(col("l1identifier"), lit(" - Progress"))).agg(first(col("l1completionPercentage")))
    assessment
      .join(leafNodes, Seq("courseid", "batchid", "userid"), "inner")
  }

  def getUserCourseInfo(fetchData: (SparkSession, Map[String, String], String, StructType, Option[Seq[String]]) => DataFrame)(implicit spark: SparkSession): DataFrame = {
    implicit val sqlContext: SQLContext = spark.sqlContext

    import sqlContext.implicits._

    val userAgg = fetchData(spark, Map("table" -> "user_activity_agg", "keyspace" -> sunbirdCoursesKeyspace), cassandraUrl, new StructType(), Some(Seq("user_id", "activity_id", "agg", "context_id")))
      .map(row => {
        UserAggData(row.getString(0), row.getString(1), row.get(2).asInstanceOf[Map[String, Int]]("completedCount"), row.getString(3))
      }).toDF()
    val hierarchyData = fetchData(spark, Map("table" -> "content_hierarchy", "keyspace" -> sunbirdHierarchyStore), cassandraUrl, new StructType(), Some(Seq("identifier", "hierarchy")))

    val hierarchyDataDf = hierarchyData.rdd.map(row => {
      val hierarchy = JSONUtils.deserialize[Map[String, AnyRef]](row.getString(1))
      CourseUtils.parseCourseHierarchy(List(hierarchy), 0, CourseData(row.getString(0), "0", List()), depthLevel = 2)
    }).toDF()

    val hierarchyDf = hierarchyDataDf.select($"courseid", $"leafNodesCount", $"level1Data", explode_outer($"level1Data").as("exploded_level1Data"))
      .select("courseid", "leafNodesCount", "exploded_level1Data.*")

    val dataDf = hierarchyDf.join(userAgg, hierarchyDf.col("courseid") === userAgg.col("activity_id"), "left")
      .withColumn("completionPercentage", (userAgg.col("completedCount") / hierarchyDf.col("leafNodesCount") * 100).cast("int"))
      .select(userAgg.col("user_id").as("userid"),
        userAgg.col("context_id").as("contextid"),
        hierarchyDf.col("courseid"),
        col("completionPercentage"),
        hierarchyDf.col("l1identifier"),
        hierarchyDf.col("l1leafNodesCount"))

    val resDf = dataDf.join(userAgg, dataDf.col("l1identifier") === userAgg.col("activity_id") &&
      userAgg.col("context_id") === dataDf.col("contextid") && userAgg.col("user_id") === dataDf.col("userid"), "left")
      .withColumn("l1completionPercentage", (userAgg.col("completedCount") / dataDf.col("l1leafNodesCount") * 100).cast("int"))
      .select(col("userid"),
        col("courseid"),
        col("contextid"),
        col("completionPercentage"),
        col("l1identifier"),
        col("l1completionPercentage"))
    resDf
  }

  def denormAssessment(report: DataFrame, contentIds: List[String])(implicit spark: SparkSession): DataFrame = {
    val contentMetaDataDF = CourseUtils.getContentNames(spark, contentIds, AppConf.getConfig("assessment.metrics.supported.contenttype"))
    report.join(contentMetaDataDF, report.col("content_id") === contentMetaDataDF.col("identifier"), "right_outer") // Doing right join since to generate report only for the "SelfAssess" content types
      .select("*")
  }

  def getAssessmentData(spark: SparkSession, loadData: (SparkSession, Map[String, String], String, StructType, Option[Seq[String]]) => DataFrame) = {
    loadData(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace),
      cassandraUrl,
      new StructType(),
      Some(Seq("course_id", "batch_id", "user_id", "content_id", "total_max_score", "total_score", "grand_total")))
      .withColumnRenamed("user_id", "userid")
      .withColumnRenamed("batch_id", "batchid")
      .withColumnRenamed("course_id", "courseid")
  }

  def getUserEnrollment(spark: SparkSession, loadData: (SparkSession, Map[String, String], String, StructType, Option[Seq[String]]) => DataFrame) = {
    loadData(spark, Map("table" -> "user_enrolments", "keyspace" -> sunbirdCoursesKeyspace),
      cassandraUrl, new StructType(),
      Some(Seq("batchid", "userid", "courseid", "active", "certificates", "enrolleddate", "completedon")))
      .withColumn("certificate_status", when(col("certificates").isNotNull && size(col("certificates").cast("array<map<string, string>>")) > 0, "Issued").otherwise(""))
      .persist(StorageLevel.MEMORY_ONLY)
  }

  def filterReportByConsent(requestedChannel: String, courseChannel: String, reportDF: DataFrame): DataFrame = {
    if (requestedChannel == courseChannel) {
      reportDF
    } else {
      reportDF.where(col("userchannel") === requestedChannel)
    }
  }

  override def saveReports(generatedreports: Dataset[OnDemandJobRequest], config: Map[String, AnyRef])(implicit spark: SparkSession, fc: FrameworkContext) = {
    generatedreports
  }

}
