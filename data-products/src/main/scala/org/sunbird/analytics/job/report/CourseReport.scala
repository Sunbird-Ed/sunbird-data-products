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
import org.sunbird.analytics.job.report.AssessmentMetricsJobV2.{finalColumnMapping, transposeDF}
import org.sunbird.analytics.util.CourseUtils.filterAssessmentDF
import org.sunbird.analytics.util.{CourseBatchInfo, CourseUtils, UserCache, UserData}
import org.sunbird.cloud.storage.conf.AppConf

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object CourseReport extends optional.Application with ReportOnDemandModelTemplate with BaseReportsJob {

  case class CourseBatchMap(batchid: String, startDate: String, endDate: String, courseChannel: String, filteres: String, courseName: String, batchName: String)
  case class  ContentBatch(courseId : String, batchId: String, startDate: String, endDate: String)
  case class Reports(requestId: String, reportPath: String, batchIds : List[ContentBatch])

  val columnsOrder = List("Batch Id", "Batch Name", "Collection Id", "Collection Name", "DIKSHA UUID", "User Name", "State", "District", "Enrolment Date", "Completion Date", "Certificate Status", "Course Progress", "Total Score")
  val reportFieldMapping = Map("courseid" -> "Collection Id", "courseName" -> "Collection Name", "batchid" -> "Batch Id", "batchName" -> "Batch Name", "userid" -> "DIKSHA UUID", "username" -> "User Name",
    "state" -> "State", "district" -> "District", "enrolleddate" -> "Enrolment Date", "completedon" -> "Completion Date", "course_completion" -> "Course Progress", "total_sum_score" -> "Total Score", "certificate_status" -> "Certificate Status"
  )

  override def filterReports(reportConfigs: DataFrame, config: Map[String, AnyRef])(implicit spark: SparkSession, fc: FrameworkContext): DataFrame = ???
//    val sunbirdCoursesKeyspace: String = AppConf.getConfig("course.metrics.cassandra.sunbirdCoursesKeyspace")
//    val sunbirdHierarchyStore: String = AppConf.getConfig("course.metrics.cassandra.sunbirdHierarchyStore")
//    val metrics: mutable.Map[String, BigInt] = mutable.Map[String, BigInt]()
//
//
//    import spark.implicits._
//    reportConfigs.show(false)
//    val batchList = List()
//    println(sunbirdCoursesKeyspace)
//    val reportEncoder = Encoders.product[Reports]
//    val activeBatches = CourseUtils.getActiveBatches(loadData, batchList, sunbirdCoursesKeyspace)
//    val filteredReports = reportConfigs.as[ReportConfigs].map(f => {
//      val contentFilters = config.getOrElse("contentFilters", Map()).asInstanceOf[Map[String, AnyRef]]
//      val applyPrivacyPolicy = true
//      val reportPath = "course-progress-reports/"
//
//      val filteredBatches = if (contentFilters.nonEmpty) {
//
//        val filteredContents = CourseUtils.filterContents(spark, JSONUtils.serialize(contentFilters)).toDF()
//        activeBatches.join(filteredContents, activeBatches.col("courseid") === filteredContents.col("identifier"), "inner")
//          .select(activeBatches.col("*")).map(f => ContentBatch(f.getString(0), f.getString(1), f.getString(2), f.getString(3))) collect
//      } else activeBatches.map(f => ContentBatch(f.getString(0), f.getString(1), f.getString(2), f.getString(3))).collect
//      Reports(f.requestId, reportPath, filteredBatches.toList)
//    })(reportEncoder)
//
//    filteredReports
//  }

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
    val filteredBatches: Array[Row] = CourseUtils.getFilteredBatches(spark, CourseUtils.getActiveBatches(fetchData, cassandraUrl, List(), sunbirdCoursesKeyspace), JSONUtils.deserialize[JobConfig](JSONUtils.serialize(config)))
    for (index <- filteredBatches.indices) {
      val row = filteredBatches(index)
      val coursesBatchInfo: CourseBatchInfo = CourseUtils.getCourseInfo(spark, row.getString(0))
      println("coursesBatchInfo" + coursesBatchInfo)
      val batchName: String = coursesBatchInfo.batches.find(x => row.getString(1) == x.batchId).map(x => x.name).getOrElse("")
      val batch = CourseBatchMap(row.getString(1), row.getString(2), row.getString(3), coursesBatchInfo.channel, "", coursesBatchInfo.name, batchName)
      val courseResponse = getCourseReport(batch = batch, userCourseInfoDF, userEnrolmentDF, assessmentDF)
    }
  }


  def getCourseReport(batch: CourseBatchMap, userCourseDF: DataFrame, userEnrollmentDF: DataFrame, assessmentProfileDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val userEnrolmentDF = userEnrollmentDF.where(col("batchid") === batch.batchid)
      .withColumn("batchName", lit(batch.batchName))
      .withColumn("courseName", lit(batch.courseName))
      .withColumn("enddate", lit(batch.endDate))
      .withColumn("startdate", lit(batch.startDate))
      .select(col("batchid"), col("userid"), col("enrolleddate"), col("completedon"), col("courseid"), col("certificate_status"),
        col("courseName"), col("batchName")
      )
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

    val assessmentAggDf = Window.partitionBy("userid", "batchid", "courseid")
    val assessDF = filterAssessmentDF(assessmentProfileDF)
      .withColumn("agg_score", sum("total_score") over assessmentAggDf)
      .withColumn("agg_max_score", sum("total_max_score") over assessmentAggDf)
      .withColumn("total_sum_score", concat(ceil((col("agg_score") * 100) / col("agg_max_score")), lit("%")))

    val assessmentDF = reportDF.join(assessDF, Seq("courseid", "batchid", "userid"), "left_outer")
    val contentIds: List[String] = assessmentDF.select(col("content_id")).distinct().collect().map(_ (0)).toList.asInstanceOf[List[String]]
    val denormedDF = denormAssessment(assessmentDF, contentIds.distinct).persist(StorageLevel.MEMORY_ONLY)
      .withColumn("username", concat_ws(" ", col("firstname"), col("lastname")))
      .select("courseid", "batchid", "userid", "username", "district", "state", "course_completion", "l1identifier", "l1completionPercentage", "content_id", "name", "grand_total", "total_sum_score", "batchName", "courseName", "certificate_status", "enrolleddate", "completedon")
    val groupedDF = denormedDF.groupBy("courseid", "batchid", "userid")
    val reportData = transposeDF(groupedDF).join(denormedDF, Seq("courseid", "batchid", "userid"), "inner")
      .dropDuplicates("userid", "courseid", "batchid")
      .drop("content_name", "null", "grand_total", "l1identifier", "l1completionPercentage", "name", "content_id")
    val customizedDF = customizeDF(reportData, reportFieldMapping, columnsOrder)
    println("customizedDF" + customizedDF.show(false))
    reportData
  }

  def transposeDF(reportDF: RelationalGroupedDataset): DataFrame = {
    val assessment = reportDF.pivot("name").agg(concat(ceil((split(first("grand_total"), "\\/")
      .getItem(0) * 100) / (split(first("grand_total"), "\\/")
      .getItem(1))), lit("%")))
    val leafNodes = reportDF.pivot(concat(col("l1identifier"), lit(" - Progress"))).agg(first(col("l1completionPercentage")))
    assessment
      .join(leafNodes, Seq("courseid", "batchid", "userid"), "inner")
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

    println("hierarchyDataDf" + hierarchyDataDf.show(20, false))

    val hierarchyDf = hierarchyDataDf.select($"courseid", $"leafNodesCount", $"level1Data", explode_outer($"level1Data").as("exploded_level1Data"))
      .select("courseid", "leafNodesCount", "exploded_level1Data.*")

    println("hierarchyDf" + hierarchyDf.show(20, false))

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
    val levelMapping = new ListBuffer[String]()
    ///levelMapping.insert(1)
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
    println("contentMetaDataDF" + contentMetaDataDF.show(20, false))
    report.join(contentMetaDataDF, report.col("content_id") === contentMetaDataDF.col("identifier"), "right_outer") // Doing right join since to generate report only for the "SelfAssess" content types
      .select("*")
  }

  /**
   * .
   * 1. Saving Reports to Blob
   * 2. Generate Metrics
   * 3. Return Map list of blobs to RequestIds as per the request
   */
  override def saveReports(generatedreports: DataFrame, config: Map[String, AnyRef])(implicit spark: SparkSession, fc: FrameworkContext): DataFrame = ???
}
