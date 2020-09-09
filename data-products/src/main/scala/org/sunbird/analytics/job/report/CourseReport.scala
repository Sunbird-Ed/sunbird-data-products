package org.sunbird.analytics.job.report

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{FrameworkContext, IJob, ReportConfigs, ReportOnDemandModelTemplate}
import org.sunbird.analytics.job.report.CourseMetricsJobV2.saveReportToBlobStore
import org.sunbird.analytics.util.{CourseBatchInfo, CourseUtils, UserCache, UserData}
import org.sunbird.cloud.storage.conf.AppConf

case class ReportLocations(requestId: String, locations: List[String])

case class CourseBatchMap(batchid: String, startDate: String, endDate: String, courseChannel: String, filteres: String, courseName: String, batchName: String)

case class ContentBatch(courseId: String, batchId: String, startDate: String, endDate: String)

case class Reports(requestId: String, reportPath: String, batchIds: List[ContentBatch])

object CourseReport extends scala.App with ReportOnDemandModelTemplate[Reports, ReportLocations, ReportLocations] with IJob with BaseReportsJob {


  override def filterReports(reportConfigs: Dataset[ReportConfigs], config: Map[String, AnyRef])(implicit spark: SparkSession, fc: FrameworkContext): Dataset[Reports] = {
    import spark.implicits._
    val activeBatches = CourseUtils.getActiveBatches(fc.loadData, cassandraUrl, List(), sunbirdCoursesKeyspace)
    val filteredReports = reportConfigs.as[ReportConfigs].collect.map(f => {
      val contentFilters = config.getOrElse("contentFilters", Map()).asInstanceOf[Map[String, AnyRef]]
      val reportPath = "course-progress-reports/"
      val filteredBatches = if (contentFilters.nonEmpty) {
        val filteredContents = CourseUtils.filterContents(spark, JSONUtils.serialize(contentFilters)).toDF()
        activeBatches.join(filteredContents, activeBatches.col("courseid") === filteredContents.col("identifier"), "inner")
          .select(activeBatches.col("*")).map(f => ContentBatch(f.getString(0), f.getString(1), f.getString(2), f.getString(3))) collect
      } else {
        println(activeBatches)
        activeBatches.show(false)
        activeBatches.map(f => ContentBatch(f.getString(0), f.getString(1), f.getString(2), f.getString(3))).collect
      }
      Reports(f.requestId, reportPath, filteredBatches.toList)

    })
    spark.createDataset(filteredReports)

  }

  override def generateReports(filteredReports: Dataset[Reports], config: Map[String, AnyRef])(implicit spark: SparkSession, fc: FrameworkContext): Dataset[ReportLocations] = {
    import spark.implicits._
    val assessmentDF = getAssessmentData(spark, loadData = fc.loadData)
    val userEnrolmentDF = getUserEnrollment(spark, loadData = fc.loadData)
    val userDF = getUserData(spark, loadData = fc.loadData)
    val container = AppConf.getConfig("cloud.container.reports")
    val objectKey = AppConf.getConfig("course.metrics.cloud.objectKey")
    val storageConfig = getStorageConfig(container, objectKey)
    val reportPath = "course-report-path/"
    val userCourseInfoDF = getUserCourseInfo(fc.loadData).join(userDF, Seq("userid"), "inner").persist(StorageLevel.MEMORY_ONLY)
    val res = filteredReports.collect().flatMap(f => {
      val reportLocation: List[ReportLocations] = f.batchIds.map(row => {
        val coursesBatchInfo: CourseBatchInfo = CourseUtils.getCourseInfo(spark, row.courseId)
        val batchName: String = coursesBatchInfo.batches.find(x => row.batchId == x.batchId).map(x => x.name).getOrElse("")
        val batch = CourseBatchMap(row.batchId, row.startDate, row.endDate, coursesBatchInfo.channel, "", coursesBatchInfo.name, batchName);
        val reportDF = getCourseReport(batch = batch, userCourseInfoDF, userEnrolmentDF, assessmentDF)
        reportDF.unpersist(true)
        val totalRecords = reportDF.count()
        val path:List[String] = reportDF.saveToBlobStore(storageConfig, "csv", reportPath + "report-" + batch.batchid, Option(Map("header" -> "true")), None)
        println("pathaa" + path)
        ReportLocations(f.requestId, path)
      })
      reportLocation
    })
    spark.createDataset(res)
  }


  def getCourseReport(batch: CourseBatchMap, userCourseDF: DataFrame, userEnrollmentDF: DataFrame, assessmentProfileDF: DataFrame)(implicit spark: SparkSession): DataFrame = {

    val columnsOrder = List("Batch Id", "Batch Name", "Collection Id", "Collection Name", "DIKSHA UUID", "User Name", "State", "District", "Enrolment Date", "Completion Date", "Certificate Status", "Course Progress", "Total Score")

    val reportFieldMapping = Map("courseid" -> "Collection Id", "courseName" -> "Collection Name", "batchid" -> "Batch Id", "batchName" -> "Batch Name", "userid" -> "DIKSHA UUID", "user_name" -> "User Name",
      "state" -> "State", "district" -> "District", "enrolleddate" -> "Enrolment Date", "completedon" -> "Completion Date", "course_completion" -> "Course Progress", "total_sum_score" -> "Total Score", "certificate_status" -> "Certificate Status"
    )

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
        col(UserCache.district), col(UserCache.state),
        col("completionPercentage").as("course_completion"),
        col("l1identifier"), col("l1completionPercentage"),
        col("user_name")
      ).persist(StorageLevel.MEMORY_ONLY)

    val assessmentAggDf = Window.partitionBy("userid", "batchid", "courseid")
    val assessDF = assessmentProfileDF
      .withColumn("agg_score", sum("total_score") over assessmentAggDf)
      .withColumn("agg_max_score", sum("total_max_score") over assessmentAggDf)
      .withColumn("total_sum_score", concat(ceil((col("agg_score") * 100) / col("agg_max_score")), lit("%")))

    val assessmentDF = reportDF.join(assessDF, Seq("courseid", "batchid", "userid"), "left_outer")
    val contentIds: List[String] = assessmentDF.select(col("content_id")).distinct().collect().map(_ (0)).toList.asInstanceOf[List[String]]
    val denormedDF = denormAssessment(assessmentDF, contentIds.distinct).persist(StorageLevel.MEMORY_ONLY)
      .select("courseid", "batchid", "userid", "user_name", "district", "state", "course_completion", "l1identifier", "l1completionPercentage", "content_id", "name", "grand_total", "total_sum_score", "batchName", "courseName", "certificate_status", "enrolleddate", "completedon")

    val groupedDF = denormedDF.groupBy("courseid", "batchid", "userid")

    val reportData = transposeDF(groupedDF).join(denormedDF, Seq("courseid", "batchid", "userid"), "inner")
      .dropDuplicates("userid", "courseid", "batchid")
      .drop("content_name", "null", "grand_total", "l1identifier", "l1completionPercentage", "name", "content_id")
    customizeDF(reportData, reportFieldMapping, columnsOrder)
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

  def getUserData(spark: SparkSession, loadData: (SparkSession, Map[String, String], String, StructType, Option[Seq[String]]) => DataFrame) = {
    val schema = Encoders.product[UserData].schema
    loadData(spark, Map("table" -> "user", "infer.schema" -> "true", "key.column" -> "userid"), redisUrl, schema,
      Some(Seq("firstname", "lastname", "userid", "state", "district"))).persist(StorageLevel.MEMORY_ONLY)
      .withColumn("user_name", concat_ws(" ", col("firstname"), col("lastname")))
  }


  override def saveReports(generatedreports: Dataset[ReportLocations], config: Map[String, AnyRef])(implicit spark: SparkSession, fc: FrameworkContext) = {
    println("Report is got saved")
    println("generatedreports" + generatedreports.show(false))
    generatedreports
  }


  override def main(config: String)(implicit sc: Option[SparkContext], fc: Option[FrameworkContext]): Unit = ???
}