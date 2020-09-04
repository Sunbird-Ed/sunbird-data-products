package org.sunbird.analytics.job.report

import java.util.concurrent.atomic.AtomicInteger

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.sunbird.analytics.job.report.AssessmentMetricsJobV2.transposeDF
import org.sunbird.analytics.util.CourseUtils.filterAssessmentDF
import org.sunbird.analytics.util.{CourseBatchInfo, CourseUtils, UserCache, UserData}
import org.sunbird.cloud.storage.conf.AppConf

object CourseReport extends optional.Application with ReportOnDemandModelTemplate with BaseReportsJob {

  case class CourseBatchMap(batchid: String, startDate: String, endDate: String, courseChannel: String, filteres: String)

  val reportFieldMapping = Map(
    "courseid" -> "Collection Id",
    "collectionname" -> "Collection Name",
    "batchid" -> "Batch Id",
    "batchname" -> "Batch Name",
    "username" -> "User Name",
    "state" -> "State",
    "district" -> "District",
    "enrolleddate" -> "Enrolment Date",
    "completedon" -> "Completion Date",
    "course_completion" -> "Course Progress",
    "total_sum_score" -> "Total Score"
  )

  /**
   * filter Reports steps before generating Report. Few pre-process steps are
   * 1. Combine or filter the report configs an
   * 2. Join or fetch Data from Tables
   */
  override def filterReports(reportConfigs: DataFrame, config: Map[String, AnyRef])(implicit spark: SparkSession, fc: FrameworkContext): DataFrame = ???

  /**
   * Method which will generate report
   * Input : List of Filtered Ids to generate Report
   */
  override def generateReports(filteredReports: DataFrame, config: Map[String, AnyRef], fetchData: (SparkSession, Map[String, String], String, Option[StructType], Option[Seq[String]]) => DataFrame)(implicit spark: SparkSession, fc: FrameworkContext): Unit = {
    val schema = Encoders.product[UserData].schema

    val userDF = fetchData(spark, Map("table" -> "user", "infer.schema" -> "true", "key.column" -> "userid"),
      redisUrl,
      Some(schema),
      Some(Seq("firstname", "lastname", "userid", "state", "district"))).withColumn("username", concat_ws(" ", col("firstname"), col("lastname"))).persist(StorageLevel.MEMORY_ONLY)

    val userEnrolmentDF = fetchData(spark, Map("table" -> "user_enrolments", "keyspace" -> sunbirdCoursesKeyspace),
      cassandraUrl, Some(new StructType()),
      Some(Seq("batchid", "userid", "courseid", "active", "certificates", "enrolleddate", "completedon")))
      .withColumn("certificate_status", when(col("certificates").isNotNull && size(col("certificates").cast("array<map<string, string>>")) > 0, "Issued").otherwise(""))
      .persist(StorageLevel.MEMORY_ONLY)

    val assessmentDF = CourseUtils.filterAssessmentDF(
      fetchData(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace),
        cassandraUrl,
        Some(new StructType()),
        Some(Seq("course_id", "batch_id", "user_id", "content_id", "total_max_score", "total_score", "grand_total")))
        .withColumnRenamed("user_id", "userid")
        .withColumnRenamed("batch_id", "batchid")
        .withColumnRenamed("course_id", "courseid")
    )
    val userCourseInfoDF = getUserCourseInfo(fetchData).join(userDF, Seq("userid"), "inner")
    println("userCourseInfoDF" + userCourseInfoDF)
    val filteredBatches: Array[Row] = CourseUtils.getFilteredBatches(spark, CourseUtils.getActiveBatches(fetchData, cassandraUrl, List(), sunbirdCoursesKeyspace), JSONUtils.deserialize[JobConfig](JSONUtils.serialize(config)))
    val totalNumOfBatches = new AtomicInteger(filteredBatches.length)
    println("totalNuofBatches" + totalNumOfBatches)
    for (index <- filteredBatches.indices) {
      val row = filteredBatches(index)
      val coursesBatchInfo: CourseBatchInfo = CourseUtils.getCourseInfo(spark, row.getString(0))
      val batch = CourseBatchMap(row.getString(1), row.getString(2), row.getString(3), coursesBatchInfo.channel, "")
      //val assessmentRes = getAssessmentReport(batch = batch, userDF = userDF, assessmentProfileDF = assessmentDF, userEnrollmentDF = userEnrolmentDF)
      //println("assessmentRes" + assessmentRes.show(false))
      val courseResponse = getCourseReport(batch = batch, userCourseInfoDF, userEnrolmentDF)
      //      if (null != coursesBatchInfo.framework && coursesBatchInfo.framework.nonEmpty) {
      //        val assessmentRes = getAssessmentReport(batch = batch, userDF = userDF, assessmentProfileDF = assessmentDF, userEnrollmentDF = userEnrolmentDF)
      //      } else {
      //        val courseResponse = getCourseReport(batch = batch, userCourseInfoDF, userEnrolmentDF)
      //      }
    }
  }


  def getCourseReport(batch: CourseBatchMap, userCourseDF: DataFrame, userEnrollmentDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val userEnrolmentDF = userEnrollmentDF.where(col("batchid") === batch.batchid)
      .withColumn("enddate", lit(batch.endDate))
      .withColumn("startdate", lit(batch.startDate))
      .select(col("batchid"), col("userid"), col("enrolleddate"), col("completedon"), col("courseid"), col("certificate_status")
      )
    println("userEnrolmentDF" + userEnrolmentDF.show(20, false))
    println("userCourseDF" + userCourseDF.show(20, false))

    val reportDF = userEnrolmentDF
      .join(userCourseDF, userCourseDF.col("contextid") === s"cb:${batch.batchid}" &&
        userEnrolmentDF.col("courseid") === userCourseDF.col("courseid") &&
        userEnrolmentDF.col("userid") === userCourseDF.col("userid"), "inner")
      .select(
        userEnrolmentDF.col("*"),
        col(UserCache.firstname), col(UserCache.lastname),
        col(UserCache.district), col(UserCache.state),
        col("completionPercentage").as("course_completion"),
        col("l1identifier"), col("l1completionPercentage")
      ).persist(StorageLevel.MEMORY_ONLY)
    println("======")
    println("reportDF" + reportDF.show(false))
    reportDF
  }

  def getAssessmentReport(batch: CourseBatchMap, userDF: DataFrame, assessmentProfileDF: DataFrame, userEnrollmentDF: DataFrame)(implicit spark: SparkSession): DataFrame = {

    val assessmentAggDf = Window.partitionBy("userid", "batchid", "courseid")
    val assessDF = filterAssessmentDF(assessmentProfileDF)
      .withColumn("agg_score", sum("total_score") over assessmentAggDf)
      .withColumn("agg_max_score", sum("total_max_score") over assessmentAggDf)
      .withColumn("total_sum_score", concat(ceil((col("agg_score") * 100) / col("agg_max_score")), lit("%")))

    val userAssesmentProfile = userEnrollmentDF.join(assessDF, Seq("userid", "batchid", "courseid"), "inner")

    val filteredAssessmentProfileDF = userAssesmentProfile.where(col("batchid") === batch.batchid)
      .withColumn("enddate", lit(batch.endDate))
      .select(col("batchid"), col("enddate"), col("userid"), col("courseid"),
        col("content_id"), col("total_score"), col("grand_total"), col("total_sum_score")
      )

    val assessmentDF = filteredAssessmentProfileDF
      .join(userDF, Seq("userid"), "inner")
      .select(filteredAssessmentProfileDF.col("*"), col(UserCache.district),
        col(UserCache.state), col("username")).persist(StorageLevel.MEMORY_ONLY)
      .filter(col("content_id").isNotNull)

    val contentIds: List[String] = assessmentDF.select(col("content_id")).distinct().collect().map(_ (0)).toList.asInstanceOf[List[String]]

    val denormedDF = denormAssessment(assessmentDF, contentIds.distinct).persist(StorageLevel.MEMORY_ONLY)
    val transposedDF = transposeDF(denormedDF)
    val reportData = transposedDF.join(denormedDF, Seq("courseid", "batchid", "userid"), "inner")
      .dropDuplicates("userid", "courseid", "batchid").drop("content_name", "courseid", "batchid", "grand_total")
    println("reportData" + reportData.show(false))
    denormedDF
  }

  def getUserCourseInfo(fetchData: (SparkSession, Map[String, String], String, Option[StructType], Option[Seq[String]]) => DataFrame)(implicit spark: SparkSession): DataFrame = {
    implicit val sqlContext: SQLContext = spark.sqlContext
    import sqlContext.implicits._
    val userAgg = fetchData(spark, Map("table" -> "user_activity_agg", "keyspace" -> sunbirdCoursesKeyspace), cassandraUrl, Some(new StructType()), Some(Seq("user_id", "activity_id", "agg", "context_id")))
      .map(row => {
        UserAggData(row.getString(0), row.getString(1), row.get(2).asInstanceOf[Map[String, Int]]("completedCount"), row.getString(3))
      }).toDF()
    val hierarchyData = fetchData(spark, Map("table" -> "content_hierarchy", "keyspace" -> sunbirdHierarchyStore), cassandraUrl, Some(new StructType()), Some(Seq("identifier", "hierarchy")))

    val hierarchyDataDf = hierarchyData.rdd.map(row => {
      val hierarchy = JSONUtils.deserialize[Map[String, AnyRef]](row.getString(1))
      parseCourseHierarchy(List(hierarchy), 0, CourseData(row.getString(0), "0", List()))
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

  def parseCourseHierarchy(data: List[Map[String, AnyRef]], levelCount: Int, prevData: CourseData): CourseData = {
    if (levelCount < 2) {
      val list = data.map(childNodes => {
        val mimeType = childNodes.getOrElse("mimeType", "").asInstanceOf[String]
        val visibility = childNodes.getOrElse("visibility", "").asInstanceOf[String]
        val contentType = childNodes.getOrElse("contentType", "").asInstanceOf[String]
        if ((StringUtils.equalsIgnoreCase(mimeType, "application/vnd.ekstep.content-collection") && StringUtils.equalsIgnoreCase(visibility, "Default") && StringUtils.equalsIgnoreCase(contentType, "Course"))) {
          val identifier = childNodes.getOrElse("identifier", "").asInstanceOf[String]
          val leafNodesCount = childNodes.getOrElse("leafNodesCount", 0).asInstanceOf[Int]
          val courseData = if (levelCount == 0) {
            CourseData(prevData.courseid, leafNodesCount.toString, List())
          } else {
            val prevL1List = prevData.level1Data
            CourseData(prevData.courseid, prevData.leafNodesCount, (prevL1List ::: List(Level1Data(identifier, leafNodesCount.toString))))
          }
          val children = childNodes.getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]]
          if (null != children && children.nonEmpty) {
            parseCourseHierarchy(children, levelCount + 1, courseData)
          } else courseData
        } else prevData
      })
      val courseId = list.head.courseid
      val leafNodeCount = list.head.leafNodesCount
      val level1Data = list.map(x => x.level1Data).flatten.toList
      CourseData(courseId, leafNodeCount, level1Data)
    } else prevData
  }

  def denormAssessment(report: DataFrame, contentIds: List[String])(implicit spark: SparkSession): DataFrame = {
    val contentMetaDataDF = CourseUtils.getContentNames(spark, contentIds, AppConf.getConfig("assessment.metrics.supported.contenttype"))
    report.join(contentMetaDataDF, report.col("content_id") === contentMetaDataDF.col("identifier"), "right_outer") // Doing right join since to generate report only for the "SelfAssess" content types
      .select(
        col("name").as("content_name"),
        col("total_sum_score"), report.col("userid"), report.col("courseid"), report.col("batchid"),
        col("grand_total"), report.col("district"),
        report.col("username"), col("state"))
  }

  /**
   * .
   * 1. Saving Reports to Blob
   * 2. Generate Metrics
   * 3. Return Map list of blobs to RequestIds as per the request
   */
  override def saveReports(generatedreports: DataFrame, config: Map[String, AnyRef])(implicit spark: SparkSession, fc: FrameworkContext): DataFrame = ???
}
